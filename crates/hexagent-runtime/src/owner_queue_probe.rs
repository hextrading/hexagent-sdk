//! Owner-local evidence for rare queue tails. No logging, allocation or new
//! worker: compact records use the preallocated advisory scheduler-tail FIFO.
//! Queue delay, overlap with the last idle wait, and current-turn work are
//! emitted with the same timestamp. These spans overlap; never sum them.
use crate::latency::{self, Instant, SchedulerTail};

const TAIL_NS: u64 = 250_000;

#[derive(Clone, Copy)]
struct Start { at: Instant, cpu_ns: u64 }

#[derive(Clone, Copy)]
struct Span { start: Start, end: Instant, end_cpu_ns: u64 }

fn ns(duration: std::time::Duration) -> u64 { duration.as_nanos().min(u64::MAX as u128) as u64 }

fn cpu_span(start: u64, end: u64) -> Option<u64> {
    (start > 0 && end >= start).then(|| end - start)
}

#[derive(Default)]
pub struct OwnerQueueProbe {
    turn: Option<Start>,
    waiting: Option<Start>,
    last_wait: Option<Span>,
}

impl OwnerQueueProbe {
    /// Startup only, on the same owner that will record observations.
    pub fn prepare() -> Self {
        latency::prepare_scheduler_tail_queue();
        Self::default()
    }

    pub fn begin_turn(&mut self) {
        self.turn = Some(Start { at: Instant::now(), cpu_ns: latency::thread_cpu_ns() });
    }

    pub fn begin_wait(&mut self) {
        self.waiting = Some(Start { at: Instant::now(), cpu_ns: latency::thread_cpu_ns() });
    }

    pub fn end_wait(&mut self) {
        if let Some(start) = self.waiting.take() {
            self.last_wait = Some(Span { start, end: Instant::now(), end_cpu_ns: latency::thread_cpu_ns() });
        }
    }

    /// Cold event rates only (private WS frames), not per BBO/quote. Fast
    /// samples do one monotonic clock read; wall/CPU clocks are read on tails.
    pub fn observe_dequeue(&self, enqueued: Instant) -> u64 {
        let now = Instant::now();
        let queue_ns = ns(now.saturating_duration_since(enqueued));
        if queue_ns < TAIL_NS { return queue_ns; }
        let observed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .map(ns).unwrap_or(0);
        for tail in self.capture(enqueued, now, latency::thread_cpu_ns(), observed) {
            latency::observe_scheduler_tail(tail);
        }
        queue_ns
    }

    fn capture(&self, enqueued: Instant, now: Instant, cpu_ns: u64, observed: u64) -> [SchedulerTail; 3] {
        let queue_ns = ns(now.saturating_duration_since(enqueued));
        let turn_ns = self.turn.map_or(0, |s| ns(now.saturating_duration_since(s.at)));
        let overlap_ns = self.last_wait.map_or(0, |s| {
            ns(s.end.min(now).saturating_duration_since(s.start.at.max(enqueued)))
        });
        let sample = |boundary, lag_ns, wall, cpu| SchedulerTail {
            probe: "private-owner-queue", boundary, observed_unix_ns: observed,
            lag_ns, span_wall_ns: wall, span_cpu_ns: cpu, expirations: 1, error_code: None,
        };
        [
            sample("live_enqueue_to_dequeue", queue_ns, queue_ns, None),
            // lag = overlap after enqueue; span = full preceding wait,
            // including Select construction, registration and wake/unwatch.
            sample("last_idle_overlap", overlap_ns,
                self.last_wait.map_or(0, |s| ns(s.end.saturating_duration_since(s.start.at))),
                self.last_wait.and_then(|s| cpu_span(s.start.cpu_ns, s.end_cpu_ns))),
            sample("current_turn_before_dequeue", turn_ns, turn_ns,
                self.turn.and_then(|s| cpu_span(s.cpu_ns, cpu_ns))),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn separates_idle_overlap_from_owner_work_without_attributing_unknown_cpu() {
        let end = Instant::now();
        let ago = |us| end - Duration::from_micros(us);
        let probe = OwnerQueueProbe {
            last_wait: Some(Span { start: Start { at: ago(1000), cpu_ns: 100 }, end: ago(20), end_cpu_ns: 300 }),
            turn: Some(Start { at: ago(19), cpu_ns: 301 }), waiting: None,
        };
        let tails = probe.capture(ago(630), end, 400, 123);
        assert_eq!(tails[0].lag_ns, 630_000);
        assert_eq!(tails[1].lag_ns, 610_000);
        assert_eq!(tails[1].span_wall_ns, 980_000);
        assert_eq!(tails[1].span_cpu_ns, Some(200));
        assert_eq!(tails[2].lag_ns, 19_000);
        assert_eq!(tails[2].span_cpu_ns, Some(99));
        assert!(tails.iter().all(|t| t.observed_unix_ns == 123));
        assert_eq!(probe.capture(ago(10), end, 0, 124)[1].lag_ns, 0);
        assert_eq!(probe.capture(ago(10), end, 0, 124)[2].span_cpu_ns, None);
        assert_eq!(OwnerQueueProbe::default().capture(ago(630), end, 400, 125)[1].lag_ns, 0);
    }

    #[test]
    fn fast_events_do_not_publish_and_owner_evidence_is_isolated() {
        std::thread::spawn(|| {
            let mut first = OwnerQueueProbe::prepare();
            first.begin_turn();
            first.observe_dequeue(Instant::now());
            assert!(latency::take_test_scheduler_tails().is_empty());
            first.observe_dequeue(Instant::now() - Duration::from_millis(2));
            let evidence = latency::take_test_scheduler_tails();
            assert_eq!(evidence.len(), 3);
            assert_eq!(evidence[0].observed_unix_ns, evidence[2].observed_unix_ns);
            std::thread::spawn(|| {
                let _second = OwnerQueueProbe::prepare();
                assert!(latency::take_test_scheduler_tails().is_empty());
            }).join().unwrap();
        }).join().unwrap();
    }
}
