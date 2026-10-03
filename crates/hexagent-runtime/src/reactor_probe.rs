//! Evidence for the general reactor shared by spot and private WebSockets.
//! Runs on that existing owner; no new thread or trading admission state.
//! Compact samples use the preallocated, lossy telemetry queues. Their sole
//! consumer is latency-dump; private/order lanes never share this capacity.
use std::{
    future::Future,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const PERIOD: Duration = Duration::from_millis(10);
const STAGE: &str = "runtime.general.deadline_lag";

pub(crate) fn prepare() {
    crate::latency::prepare_scheduler_tail_queue();
    crate::latency::prepare_observation_stages(&[STAGE]);
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn cpu_delta(previous: u64, current: u64) -> Option<u64> {
    (previous != 0 && current >= previous).then(|| current - previous)
}

/// A pending HTTP future is not a busy reactor. Time individual polls, not
/// the lifetime of the future. The type name identifies the cold call site
/// without allocating or formatting on the reactor. This wrapper is used
/// only by the existing synchronous general-runtime bridge, never orders.
pub(crate) async fn observe_task<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        let start = Instant::now();
        let cpu = crate::latency::thread_cpu_ns();
        let result = future.as_mut().poll(cx);
        let elapsed = start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        if elapsed >= 5_000_000 {
            crate::latency::observe_scheduler_tail(crate::latency::SchedulerTail {
                probe: std::any::type_name::<F>(),
                observed_unix_ns: unix_ns(),
                lag_ns: elapsed,
                span_wall_ns: elapsed,
                span_cpu_ns: cpu_delta(cpu, crate::latency::thread_cpu_ns()),
                expirations: 0,
                error_code: None,
                boundary: "single_future_poll",
            });
        }
        result
    })
    .await
}

/// Timerfd measures from the first missed kernel deadline, not the latest
/// coalesced tick. The portable fallback is explicitly labeled and retains
/// a 5ms threshold to avoid treating timer-wheel rounding as a scheduler tail.
pub(crate) async fn run() {
    let mut precise = crate::precise_interval::PreciseInterval::new(PERIOD).ok();
    log::info!(
        "[general_reactor_probe] backend={} interval_ms=10 poll_tail_threshold_ms=5",
        if precise.is_some() {
            "timerfd"
        } else {
            "tokio_interval"
        }
    );
    let mut portable = tokio::time::interval_at(tokio::time::Instant::now() + PERIOD, PERIOD);
    portable.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous = Instant::now();
    let mut previous_cpu = crate::latency::thread_cpu_ns();
    loop {
        let (probe, lag, expirations, threshold) = if let Some(timer) = precise.as_mut() {
            match timer.tick().await {
                Ok(tick) => (
                    "general_timerfd",
                    tick.lag_ns(),
                    tick.expirations,
                    1_000_000,
                ),
                Err(error) => {
                    crate::latency::observe_scheduler_tail(crate::latency::SchedulerTail {
                        probe: "general_timerfd_error",
                        observed_unix_ns: unix_ns(),
                        lag_ns: 0,
                        span_wall_ns: 0,
                        span_cpu_ns: None,
                        expirations: 0,
                        error_code: error.raw_os_error(),
                        boundary: "timer_read_error",
                    });
                    precise = None;
                    portable.reset();
                    previous = Instant::now();
                    previous_cpu = crate::latency::thread_cpu_ns();
                    continue;
                }
            }
        } else {
            (
                "general_tokio_interval",
                portable
                    .tick()
                    .await
                    .elapsed()
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
                1,
                5_000_000,
            )
        };
        let now = Instant::now();
        let cpu = crate::latency::thread_cpu_ns();
        crate::latency::observe_ns(STAGE, lag);
        if lag >= threshold {
            crate::latency::observe_scheduler_tail(crate::latency::SchedulerTail {
                probe,
                observed_unix_ns: unix_ns(),
                lag_ns: lag,
                span_wall_ns: now
                    .duration_since(previous)
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
                span_cpu_ns: cpu_delta(previous_cpu, cpu),
                expirations,
                error_code: None,
                boundary: "previous_actual_probe_to_current",
            });
        }
        previous = now;
        previous_cpu = cpu;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_or_backward_cpu_clock_remains_unknown() {
        assert_eq!(cpu_delta(0, 1), None);
        assert_eq!(cpu_delta(2, 1), None);
        assert_eq!(cpu_delta(2, 12), Some(10));
    }

    #[test]
    fn task_polling_preserves_pending_wakes_output_and_cancellation() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut polls = 0;
            let value = observe_task(std::future::poll_fn(|cx| {
                polls += 1;
                if polls == 1 {
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                } else {
                    std::task::Poll::Ready(17)
                }
            }))
            .await;
            assert_eq!((value, polls), (17, 2));
            assert!(tokio::time::timeout(
                Duration::from_millis(1),
                observe_task(std::future::pending::<()>())
            )
            .await
            .is_err());
        });
    }

    #[test]
    fn stalled_owner_emits_deadline_and_single_poll_evidence() {
        // A real reactor stall, including OS sleep, must be visible even with
        // no market callback. This is deliberately not a trading-path test.
        std::thread::spawn(|| {
            prepare();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let probe = tokio::spawn(run());
                tokio::task::yield_now().await;
                observe_task(async {
                    std::thread::sleep(Duration::from_millis(50));
                })
                .await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                probe.abort();
                let tails = crate::latency::take_test_scheduler_tails();
                assert!(tails
                    .iter()
                    .any(|t| t.boundary == "single_future_poll" && t.span_wall_ns >= 40_000_000));
                assert!(tails
                    .iter()
                    .any(|t| t.boundary == "previous_actual_probe_to_current"
                        && t.lag_ns >= 20_000_000));
                assert!(crate::latency::take_test_scheduler_tails().is_empty());
            });
        })
        .join()
        .unwrap();
    }

    #[test]
    #[ignore = "focused release benchmark; reports synchronous general-task poll overhead"]
    fn general_poll_overhead() {
        let mut baseline = Vec::with_capacity(10_000);
        let mut observed = Vec::with_capacity(10_000);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            for _ in 0..10_000 {
                let start = Instant::now();
                std::hint::black_box(std::future::ready(std::hint::black_box(17)).await);
                baseline.push(start.elapsed().as_nanos());
                let start = Instant::now();
                std::hint::black_box(
                    observe_task(std::future::ready(std::hint::black_box(17))).await,
                );
                observed.push(start.elapsed().as_nanos());
            }
        });
        for (name, mut values) in [("baseline", baseline), ("observed", observed)] {
            values.sort_unstable();
            eprintln!("{name}: n={} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 (no tail emitted)",
                values.len(), values[4999], values[9899], values[9989], values[9999]);
        }
    }
}
