use super::*;
use std::future::{pending, ready};
use tokio::time::{advance, Instant};

async fn ping_timer() -> tokio::time::Interval {
    let mut ping = tokio::time::interval(PING_INTERVAL);
    ping.tick().await;
    ping
}

// Exact old selection/cancellation shape: an outbound tick drops the read
// timeout and the next iteration starts an entirely new timeout.
async fn old_select<F: std::future::Future>(
    inbound: F,
    ping: &mut tokio::time::Interval,
    threshold: Duration,
) -> SocketAction<F::Output> {
    tokio::select! {
        biased;
        _ = ping.tick() => SocketAction::Ping,
        result = tokio::time::timeout(threshold, inbound) => match result {
            Ok(frame) => SocketAction::Inbound(frame),
            Err(_) => SocketAction::Stalled,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn old_ping_loop_reproduces_suppressed_spot_and_futures_watchdog() {
    for threshold in [STALE_THRESHOLD_SPOT, STALE_THRESHOLD_FUTURES] {
        let started = Instant::now();
        let mut ping = ping_timer().await;
        for _ in 0..5 {
            assert!(matches!(
                old_select(pending::<()>(), &mut ping, threshold).await,
                SocketAction::Ping
            ));
        }
        assert_eq!(Instant::now() - started, Duration::from_secs(150));
    }
}

#[tokio::test(start_paused = true)]
async fn absolute_inbound_deadline_survives_outbound_pings() {
    for (threshold, expected_pings) in [(STALE_THRESHOLD_SPOT, 0), (STALE_THRESHOLD_FUTURES, 2)] {
        let started = Instant::now();
        let mut ping = ping_timer().await;
        let deadline = InboundDeadline::new(threshold);
        let mut sent = 0;
        loop {
            match deadline.next(pending::<()>(), &mut ping).await {
                SocketAction::Ping => {
                    sent += 1;
                    deadline.control_write(ready(())).await.unwrap();
                }
                SocketAction::Stalled => break,
                SocketAction::Inbound(_) => panic!("no peer frame was sent"),
            }
        }
        assert_eq!(sent, expected_pings);
        assert_eq!(Instant::now() - started, threshold);
    }
}

#[tokio::test(start_paused = true)]
async fn received_text_ping_and_pong_renew_transport_deadline_only() {
    // Parsing and depth validity are deliberately outside this transport
    // watchdog: even malformed Text, Ping and Pong show inbound activity.
    for message in [
        Message::Text("not market data".into()),
        Message::Ping(Vec::new()),
        Message::Pong(Vec::new()),
    ] {
        let started = Instant::now();
        let mut ping = ping_timer().await;
        let mut deadline = InboundDeadline::new(STALE_THRESHOLD_SPOT);
        advance(Duration::from_secs(20)).await;
        assert!(matches!(
            deadline.next(ready(message), &mut ping).await,
            SocketAction::Inbound(_)
        ));
        deadline.received(); // production calls this for each successful frame
        assert_eq!(deadline.expires_at, started + Duration::from_secs(50));
        assert!(matches!(
            deadline.next(pending::<()>(), &mut ping).await,
            SocketAction::Ping
        ));
        assert!(matches!(
            deadline.next(pending::<()>(), &mut ping).await,
            SocketAction::Stalled
        ));
        assert_eq!(Instant::now() - started, Duration::from_secs(50));
    }
}

#[tokio::test(start_paused = true)]
async fn pending_control_write_cannot_outlive_inbound_deadline() {
    let mut deadline = InboundDeadline::new(STALE_THRESHOLD_SPOT);
    let started = Instant::now();
    advance(Duration::from_secs(20)).await;
    assert!(deadline.control_write(pending::<()>()).await.is_err());
    assert_eq!(Instant::now() - started, STALE_THRESHOLD_SPOT);
    // An inbound server Ping renews the deadline before its Pong write. The
    // Pong itself must still time out if its sink stops accepting writes.
    deadline.received();
    assert!(deadline.control_write(pending::<()>()).await.is_err());
    assert_eq!(Instant::now() - started, Duration::from_secs(60));
}

#[tokio::test(start_paused = true)]
async fn expired_deadline_wins_simultaneous_ping_and_ready_frame() {
    let mut ping = ping_timer().await;
    let deadline = InboundDeadline::new(STALE_THRESHOLD_SPOT);
    advance(STALE_THRESHOLD_SPOT).await;
    assert!(matches!(
        deadline.next(ready(()), &mut ping).await,
        SocketAction::Stalled
    ));
}

#[tokio::test(start_paused = true)]
async fn socket_deadlines_are_isolated_and_reconnect_starts_fresh() {
    let started = Instant::now();
    let mut first = InboundDeadline::new(STALE_THRESHOLD_SPOT);
    let second = InboundDeadline::new(STALE_THRESHOLD_SPOT);
    advance(Duration::from_secs(10)).await;
    first.received();
    assert_eq!(first.expires_at, started + Duration::from_secs(40));
    assert_eq!(second.expires_at, started + Duration::from_secs(30));
    advance(Duration::from_secs(20)).await;
    let reconnect = InboundDeadline::new(STALE_THRESHOLD_SPOT);
    assert_eq!(reconnect.expires_at, started + Duration::from_secs(60));
}

#[test]
#[ignore = "focused timer/select CPU benchmark, no network or queues"]
fn ready_read_deadline_before_after_benchmark() {
    const N: usize = 100_000;
    const WARMUP: usize = 5_000;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut old_ping = ping_timer().await;
        let mut new_ping = ping_timer().await;
        let mut deadline = InboundDeadline::new(STALE_THRESHOLD_SPOT);
        let mut old = Vec::with_capacity(N);
        let mut new = Vec::with_capacity(N);
        for index in 0..N + WARMUP {
            for is_new in if index % 2 == 0 { [false, true] } else { [true, false] } {
                let start = std::time::Instant::now();
                let action = if is_new {
                    let action = deadline.next(ready(std::hint::black_box(())), &mut new_ping).await;
                    deadline.received();
                    action
                } else {
                    old_select(ready(std::hint::black_box(())), &mut old_ping, STALE_THRESHOLD_SPOT).await
                };
                let elapsed = start.elapsed().as_nanos() as u64;
                assert!(matches!(action, SocketAction::Inbound(())));
                if index >= WARMUP {
                    if is_new { new.push(elapsed); } else { old.push(elapsed); }
                }
            }
        }
        for (version, mut samples) in [("old", old), ("absolute_inbound_deadline", new)] {
            samples.sort_unstable();
            let at = |q: f64| samples[((samples.len() - 1) as f64 * q).ceil() as usize];
            println!("BINANCE_DEADLINE_BENCH {{\"version\":\"{version}\",\"n\":{N},\"warmup\":{WARMUP},\"unit\":\"ns\",\"p50\":{},\"p99\":{},\"p999\":{},\"max\":{},\"queue_depth\":null,\"overflow\":null}}", at(0.5), at(0.99), at(0.999), samples.last().unwrap());
        }
    });
}
