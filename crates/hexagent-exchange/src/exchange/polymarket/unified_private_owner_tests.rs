use super::super::trade::with_private_owner_mode;
use super::execution_route_tests::{route_event, route_fixture};
use super::*;

fn fixture(
    unified: bool,
    capacity: usize,
) -> (
    Arc<SharedState>,
    PrivateApplyLane,
    crossbeam_channel::Receiver<RoutedOrderUpdate>,
    Arc<AtomicBool>,
    Vec<std::thread::JoinHandle<()>>,
) {
    let shared = with_private_owner_mode(unified, route_fixture);
    shared.user_feed_health.set_recovering(false);
    let (tx, rx) = crossbeam_channel::bounded(capacity);
    let stop = Arc::new(AtomicBool::new(false));
    let (lane, workers) = spawn_private_apply_worker(shared.clone(), tx, stop.clone()).unwrap();
    assert_eq!(
        workers.len(),
        usize::from(!unified),
        "unified mode must not spawn a second FIFO ingress thread"
    );
    (shared, lane, rx, stop, workers)
}

fn recv(rx: &crossbeam_channel::Receiver<RoutedOrderUpdate>) -> RoutedOrderUpdate {
    rx.recv_timeout(Duration::from_secs(5))
        .expect("private update")
}

#[test]
fn unified_large_frame_preserves_order_and_duplicates_across_replay() {
    let (shared, lane, rx, _, _) = fixture(true, 16);
    let events: Vec<_> = (0..3)
        .map(|i| route_event(2, &format!("ordered-{i}")))
        .collect();
    lane.dispatch_live(events.clone(), None).unwrap();
    for i in 0..3 {
        assert_eq!(
            recv(&rx).update.trade_id.as_deref(),
            Some(format!("ordered-{i}").as_str())
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        timeout(
            Duration::from_secs(5),
            lane.apply_replay_batch(events, None),
        )
        .await
        .unwrap()
        .unwrap();
    });
    assert!(rx.is_empty(), "replay must not reapply a delivered trade");
    let snapshot = shared.account_state.instance_snapshot("owner-1").unwrap();
    assert!((snapshot.positions["DOWN"] - 55.0).abs() < 1e-9);
}

#[test]
fn unified_backpressure_retains_private_updates_and_does_not_block_lifecycle_control() {
    let (shared, lane, rx, _, _) = fixture(true, 1);
    lane.dispatch_live(
        vec![
            route_event(2, "retained-1"),
            route_event(2, "retained-2"),
            route_event(2, "retained-3"),
        ],
        None,
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while shared.user_feed_health.new_orders_ready() {
        assert!(
            Instant::now() < deadline,
            "backpressure must pause new orders"
        );
        std::thread::yield_now();
    }
    assert_eq!(
        shared
            .user_feed_health
            .current_recovery_delivery_generation(),
        Ok(None),
        "backpressure must not invent a reconnect epoch"
    );
    // A recovery control request shares the account owner. With the old
    // blocking delivery this would wait for the downstream queue's timeout.
    let started = Instant::now();
    let generation = shared.user_feed_health.begin_recovery_delivery();
    assert_ne!(generation, 0);
    assert!(started.elapsed() < Duration::from_secs(1));
    for id in ["retained-1", "retained-2", "retained-3"] {
        assert_eq!(recv(&rx).update.trade_id.as_deref(), Some(id));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        shared.user_feed_health.set_recovering(false);
        if shared.user_feed_health.new_orders_ready() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "drained outbox must release its pause"
        );
        std::thread::yield_now();
    }
    assert_eq!(lane.reconnect_generation.load(Ordering::Acquire), 0);
}

#[test]
fn unified_accounts_do_not_block_or_deliver_into_each_other() {
    let (_a, lane_a, rx_a, _, _) = fixture(true, 1);
    let (_b, lane_b, rx_b, _, _) = fixture(true, 8);
    let (_c, lane_c, rx_c, _, _) = fixture(true, 8);
    lane_a
        .dispatch_live(vec![route_event(1, "a-1"), route_event(1, "a-2")], None)
        .unwrap();
    lane_b
        .dispatch_live(vec![route_event(1, "b-1")], None)
        .unwrap();
    lane_c
        .dispatch_live(vec![route_event(1, "c-1")], None)
        .unwrap();
    assert_eq!(recv(&rx_b).update.trade_id.as_deref(), Some("b-1"));
    assert_eq!(recv(&rx_c).update.trade_id.as_deref(), Some("c-1"));
    assert_eq!(recv(&rx_a).update.trade_id.as_deref(), Some("a-1"));
    assert_eq!(recv(&rx_a).update.trade_id.as_deref(), Some("a-2"));
    assert!(rx_b.is_empty() && rx_c.is_empty());
}

#[test]
fn unified_recovery_fence_follows_entire_frame_and_strategy_ack() {
    let (shared, lane, rx, _, _) = fixture(true, 8);
    let generation = shared.user_feed_health.begin_recovery_delivery();
    lane.dispatch_live(
        vec![route_event(2, "fence-1"), route_event(2, "fence-2")],
        Some(generation),
    )
    .unwrap();
    let (completion, mut fence) = tokio::sync::oneshot::channel();
    lane.live_tx
        .try_send(PrivateApplyCommand::RecoveryFence {
            generation,
            completion,
        })
        .unwrap();
    let one = recv(&rx);
    let two = recv(&rx);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(finished) = fence.try_recv() {
            assert!(finished);
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(
        shared
            .user_feed_health
            .recovery_delivery_progress(generation),
        Some((true, 2))
    );
    assert!(shared
        .user_feed_health
        .acknowledge_recovery_update("owner-1", &one.update));
    assert!(shared
        .user_feed_health
        .acknowledge_recovery_update("owner-1", &two.update));
    assert_eq!(
        shared
            .user_feed_health
            .recovery_delivery_progress(generation),
        Some((true, 0))
    );
    let next = shared.user_feed_health.begin_recovery_delivery();
    assert_ne!(next, generation);
    assert!(!shared
        .user_feed_health
        .finish_recovery_delivery_enrollment(generation));
}

#[test]
fn unified_empty_replay_still_rejects_a_stale_recovery_certificate() {
    let (shared, lane, rx, _, _) = fixture(true, 8);
    let old_certificate = shared.user_feed_health.recovery_certificate();
    let generation = shared.user_feed_health.begin_recovery_delivery();
    let (completion, result) = tokio::sync::oneshot::channel();
    lane.replay_tx
        .try_send(PrivateApplyCommand::Replay {
            events: Vec::new(),
            recovery_generation: Some(generation),
            expected_recovery_certificate: Some(old_certificate),
            completion,
        })
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    assert!(runtime
        .block_on(async {
            timeout(Duration::from_secs(5), result)
                .await
                .unwrap()
                .unwrap()
        })
        .is_err());
    assert!(rx.is_empty());
    assert!(!shared.user_feed_health.new_orders_ready());
}

#[test]
fn unified_full_ingress_lane_fails_closed_with_explicit_replay_error() {
    let shared = with_private_owner_mode(true, route_fixture);
    let (tx, _rx) = crossbeam_channel::bounded(1);
    let (lane, _uninstalled_owner) =
        unified_private_owner::prepare(&shared, tx, Arc::new(AtomicBool::new(false))).unwrap();
    for _ in 0..PRIVATE_APPLY_QUEUE_CAPACITY {
        lane.dispatch_live(vec![route_event(1, "queued")], None)
            .unwrap();
    }
    assert!(lane
        .dispatch_live(vec![route_event(1, "overflow")], None)
        .is_err());
    assert_eq!(lane.live_tx.len(), PRIVATE_APPLY_QUEUE_CAPACITY);
}

#[test]
#[ignore = "focused split/unified three-account benchmark, run alone in release"]
fn benchmark_three_account_private_owners() {
    crate::os_tune::init_disabled();
    const EVENTS: usize = 4096;
    const BURST: usize = 32;
    for unified in [false, true] {
        let mut accounts = Vec::new();
        for _ in 0..3 {
            let (shared, lane, rx, stop, workers) = fixture(unified, BURST * 2);
            let (commit, committed) = crossbeam_channel::bounded(BURST * 2);
            shared.private_commit_benchmark.set(commit).unwrap();
            // Build and classify all payloads before the measured boundary.
            let events: Vec<_> = (0..EVENTS)
                .map(|i| {
                    let mut event = route_event(1, &format!("bench-{i}"));
                    event.payload["size"] = serde_json::json!("0.001");
                    event.payload["maker_orders"][0]["matched_amount"] = serde_json::json!("0.001");
                    event
                })
                .collect();
            accounts.push((
                shared,
                lane,
                rx,
                stop,
                workers,
                committed,
                events.into_iter(),
            ));
        }
        let mut routed = Vec::with_capacity(EVENTS * 3);
        let mut producer_ready = Vec::with_capacity(EVENTS * 3);
        let mut applied = Vec::with_capacity(EVENTS * 3);
        let mut live_high = 0;
        let mut output_high = 0;
        for _ in 0..EVENTS / BURST {
            for (_, lane, _, _, _, _, events) in &mut accounts {
                let mut frame: Vec<_> = events.by_ref().take(BURST).collect();
                let start = crate::types::monotonic_now_ns();
                for event in &mut frame {
                    event.timing.private_ws_received_ns = start;
                }
                lane.dispatch_live(frame, None).unwrap();
                live_high = live_high.max(lane.live_tx.len());
            }
            for (_, _, rx, _, _, committed, _) in &accounts {
                output_high = output_high.max(rx.len());
                for _ in 0..BURST {
                    let update = recv(rx);
                    assert!(
                        update.timing.private_producer_ns >= update.timing.private_ws_received_ns
                    );
                    producer_ready.push(
                        update.timing.private_producer_ns - update.timing.private_ws_received_ns,
                    );
                    routed.push(
                        crate::types::monotonic_now_ns()
                            .saturating_sub(update.timing.private_ws_received_ns),
                    );
                }
                for _ in 0..BURST {
                    applied.push(committed.recv_timeout(Duration::from_secs(5)).unwrap());
                }
            }
        }
        let stats = |mut samples: Vec<u64>| {
            samples.sort_unstable();
            let q = |p: usize| samples[(samples.len() * p).div_ceil(1000) - 1];
            serde_json::json!({"n":samples.len(), "p50_ns":q(500), "p99_ns":q(990), "p999_ns":q(999), "max_ns":samples.last().unwrap()})
        };
        eprintln!(
            "{}",
            serde_json::json!({
                "mode":if unified {"unified"} else {"split"}, "accounts":3, "burst_per_account":BURST,
                "boundary_start":"classified_frame_before_private_lane_enqueue",
                "producer_ready":stats(producer_ready),
                "routed_consumer":stats(routed), "local_lifecycle_applied":stats(applied),
                "live_lane_capacity":PRIVATE_APPLY_QUEUE_CAPACITY,"live_lane_high_water":live_high,
                "output_lane_capacity":BURST*2,"output_lane_sampled_high_water":output_high,
                "overflow":0,"network_included":false,"strategy_application_included":false,"durable_flush_included":false,
                "pinning_active":false,"fifo_active":false
            })
        );
        for (shared, _, _, stop, workers, _, _) in accounts {
            assert!(
                (shared
                    .account_state
                    .instance_snapshot("owner-1")
                    .unwrap()
                    .positions["DOWN"]
                    - (100.0 - EVENTS as f64 * 0.001))
                    .abs()
                    < 1e-6
            );
            stop.store(true, Ordering::Release);
            for worker in workers {
                worker.join().unwrap();
            }
        }
    }
}
