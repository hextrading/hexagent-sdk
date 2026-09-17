use super::super::live_position::UserFeedHealth;
use super::*;
use std::cell::Cell;
use std::task::Poll;

#[tokio::test(start_paused = true)]
async fn drained_orders_retry_within_one_probe_instead_of_thirty_seconds() {
    let start = tokio::time::Instant::now();
    let deadline = start + Duration::from_secs(30);
    let mut next = start;
    let nonempty = Cell::new(true);
    let shutdown = AtomicBool::new(false);
    let health = UserFeedHealth::default();
    health.set_recovering(true);
    let mut old = Box::pin(tokio::time::sleep_until(deadline));
    let mut retry = Box::pin(wait_for_order_audit_retry(
        deadline,
        true,
        &mut next,
        || nonempty.get(),
        &shutdown,
    ));
    assert!(futures_util::poll!(old.as_mut()).is_pending());
    assert!(futures_util::poll!(retry.as_mut()).is_pending());
    tokio::time::advance(Duration::from_millis(50)).await;
    nonempty.set(false);
    tokio::time::advance(Duration::from_millis(50)).await;
    assert_eq!(retry.await.unwrap(), RecoveryRetryWake::OpenOrdersDrained);
    assert_eq!(
        tokio::time::Instant::now() - start,
        Duration::from_millis(100)
    );
    assert!(futures_util::poll!(old.as_mut()).is_pending());
    assert!(
        health.is_recovering(),
        "a retry hint cannot clear the delivery gate"
    );
    tokio::time::advance(Duration::from_millis(29_900)).await;
    old.await;
}

#[tokio::test(start_paused = true)]
async fn unchanged_and_already_empty_failures_keep_rest_backoff() {
    let shutdown = AtomicBool::new(false);
    for (started_nonempty, still_nonempty) in [(true, true), (false, false)] {
        let start = tokio::time::Instant::now();
        let deadline = start + Duration::from_secs(30);
        let mut next = start;
        let reads = Cell::new(0);
        let mut retry = Box::pin(wait_for_order_audit_retry(
            deadline,
            started_nonempty,
            &mut next,
            || {
                reads.set(reads.get() + 1);
                still_nonempty
            },
            &shutdown,
        ));
        assert!(futures_util::poll!(retry.as_mut()).is_pending());
        tokio::time::advance(Duration::from_millis(29_999)).await;
        assert!(futures_util::poll!(retry.as_mut()).is_pending());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(retry.await.unwrap(), RecoveryRetryWake::Deadline);
        if !started_nonempty {
            assert_eq!(reads.get(), 0, "empty failed audits must not hot-loop");
        }
    }
}

#[tokio::test(start_paused = true)]
async fn socket_select_cancellation_retains_the_bounded_probe_deadline() {
    let start = tokio::time::Instant::now();
    let deadline = start + Duration::from_secs(30);
    let mut next = start;
    let reads = Cell::new(0);
    let nonempty = Cell::new(true);
    let shutdown = AtomicBool::new(false);
    for _ in 0..500 {
        let mut retry = Box::pin(wait_for_order_audit_retry(
            deadline,
            true,
            &mut next,
            || {
                reads.set(reads.get() + 1);
                nonempty.get()
            },
            &shutdown,
        ));
        assert!(futures_util::poll!(retry.as_mut()).is_pending());
        // Dropping the pending arm models each private frame winning select.
    }
    assert_eq!(reads.get(), 1);
    nonempty.set(false);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(
        wait_for_order_audit_retry(
            deadline,
            true,
            &mut next,
            || {
                reads.set(reads.get() + 1);
                nonempty.get()
            },
            &shutdown
        )
        .await
        .unwrap(),
        RecoveryRetryWake::OpenOrdersDrained
    );
    assert_eq!(reads.get(), 2);
}

#[tokio::test(start_paused = true)]
async fn independent_accounts_and_shutdown_do_not_share_retry_hints() {
    let start = tokio::time::Instant::now();
    let deadline = start + Duration::from_secs(30);
    let mut first_check = start;
    let mut second_check = start;
    let shutdown = AtomicBool::new(false);
    assert_eq!(
        wait_for_order_audit_retry(deadline, true, &mut first_check, || false, &shutdown)
            .await
            .unwrap(),
        RecoveryRetryWake::OpenOrdersDrained
    );
    let mut second = Box::pin(wait_for_order_audit_retry(
        deadline,
        true,
        &mut second_check,
        || true,
        &shutdown,
    ));
    assert!(matches!(
        futures_util::poll!(second.as_mut()),
        Poll::Pending
    ));
    shutdown.store(true, Ordering::Relaxed);
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(second.await.unwrap_err().to_string().contains("shutdown"));
}

#[test]
#[ignore = "local selection-boundary microbenchmark; no network or live trading"]
fn recovery_retry_ready_private_frame_benchmark() {
    let shared = super::tests::test_shared();
    shared
        .track_open_order(
            "recovery-benchmark",
            super::super::trade::TrackedOrder {
                order_slot: Default::default(),
                symbol: "TOKEN".into(),
                side: Side::Buy,
                instance_id: "owner-1".into(),
            },
        )
        .unwrap();
    shared.flush_execution_state_for_test();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        for progress_hint in [false, true] {
            let (tx, rx) = crossbeam_channel::bounded(1);
            let shutdown = AtomicBool::new(false);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
            let mut next = tokio::time::Instant::now();
            let mut values = Vec::with_capacity(10_000);
            let mut high = 0;
            for sequence in 0..10_000 {
                tx.try_send(sequence).unwrap();
                high = high.max(rx.len());
                let started = Instant::now();
                let received = if progress_hint {
                    matches!(select_ws_or_recovery(std::future::ready(rx.try_recv()),
                        wait_for_order_audit_retry(deadline, true, &mut next,
                            || shared.execution_snapshot().open_orders.len() != 0, &shutdown)).await,
                        RecoveryReadEvent::Socket(Ok(value)) if value == sequence)
                } else {
                    matches!(select_ws_or_recovery(std::future::ready(rx.try_recv()),
                        tokio::time::sleep_until(deadline)).await,
                        RecoveryReadEvent::Socket(Ok(value)) if value == sequence)
                };
                values.push(started.elapsed().as_nanos());
                assert!(received);
            }
            values.sort_unstable();
            eprintln!("recovery_retry_select progress_hint={progress_hint} n={} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth={} queue_high_water={high} overflow=0",
                values.len(), values[5000], values[9900], values[9990], values[9999], rx.len());
        }
    });
}
