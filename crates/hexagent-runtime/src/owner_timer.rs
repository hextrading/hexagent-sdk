//! Periodic deadlines owned by one thread. Unlike channel tickers, these
//! never access AtomicCell<Instant>'s process-wide fallback locks.
use std::time::{Duration, Instant};

pub struct OwnerTimer {
    period: Duration,
    next: Instant,
}

impl OwnerTimer {
    pub fn new(period: Duration, now: Instant) -> Self {
        assert!(!period.is_zero());
        Self {
            period,
            next: now + period,
        }
    }

    pub fn remaining(&self, now: Instant) -> Duration {
        self.next.saturating_duration_since(now)
    }

    /// Coalesce missed ticks into one turn, retaining the former tick
    /// channel's now+period schedule instead of replaying an overdue burst.
    pub fn take_due(&mut self, now: Instant) -> bool {
        if now < self.next {
            return false;
        }
        self.next = now + self.period;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_deadline_and_delayed_turn_coalesce_without_catchup() {
        let now = Instant::now();
        let period = Duration::from_millis(100);
        let mut timer = OwnerTimer::new(period, now);
        assert!(!timer.take_due(now + period - Duration::from_nanos(1)));
        assert!(timer.take_due(now + period));
        assert!(!timer.take_due(now + period));
        let delayed = now + Duration::from_secs(2);
        assert_eq!(timer.remaining(delayed), Duration::ZERO);
        assert!(timer.take_due(delayed));
        assert_eq!(timer.remaining(delayed), period);
        assert!(!timer.take_due(delayed));
    }

    #[test]
    fn independent_owners_do_not_share_deadlines() {
        let now = Instant::now();
        let period = Duration::from_millis(100);
        let mut first = OwnerTimer::new(period, now);
        let mut second = OwnerTimer::new(period, now);
        assert!(first.take_due(now + period));
        assert_eq!(second.remaining(now + period), Duration::ZERO);
        assert!(second.take_due(now + period));
    }
    /// Focused timer/selection benchmark, not network or end-to-end latency.
    /// One replaceable event is published before each loop turn; both variants
    /// retain the same bounded queue and private-event priority.
    #[test]
    #[ignore = "manual timer/select benchmark; run release with --ignored --nocapture"]
    fn owner_timer_router_turn_benchmark() {
        const N: usize = 200_000;
        let queue = crate::try_queue::TryQueue::new(64);
        let (_private_tx, private_rx) = crossbeam_channel::bounded::<()>(64);
        let (_executor_tx, executor_rx) = crossbeam_channel::bounded::<()>(64);
        let tick = crossbeam_channel::tick(Duration::from_millis(100));
        let retry_tick = crossbeam_channel::tick(Duration::from_micros(50));
        let mut owner_timer = OwnerTimer::new(Duration::from_millis(100), Instant::now());
        for before in [true, false] {
            let mut samples = Vec::with_capacity(N);
            for value in 0..N {
                queue.try_push(value).unwrap();
                let start = Instant::now();
                loop {
                    let mut received = None;
                    if before {
                        crossbeam_channel::select_biased! {
                            recv(private_rx) -> _ => unreachable!(),
                            recv(executor_rx) -> _ => unreachable!(),
                            recv(tick) -> _ => {},
                            recv(retry_tick) -> _ => {},
                            default(Duration::ZERO) => { received = queue.try_pop(); },
                        }
                    } else {
                        std::hint::black_box(owner_timer.take_due(Instant::now()));
                        crossbeam_channel::select_biased! {
                            recv(private_rx) -> _ => unreachable!(),
                            recv(executor_rx) -> _ => unreachable!(),
                            default(Duration::ZERO) => { received = queue.try_pop(); },
                        }
                    }
                    if let Some(received) = received {
                        assert_eq!(received, value);
                        break;
                    }
                }
                samples.push(start.elapsed().as_nanos());
            }
            samples.sort_unstable();
            eprintln!("timer_select_pop before={} n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water=1 overflow=0",
                before, N, samples[N/2], samples[N*99/100-1], samples[N*999/1000-1], samples[N-1]);
        }
    }
}
