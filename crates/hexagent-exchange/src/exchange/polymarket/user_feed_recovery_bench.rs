//! Offline recovery coordination regression and focused latency evidence.
//!
//! These tests exercise the production socket/audit selection and live enqueue
//! functions. The fake futures model backend readiness, not network latency.
//! The benchmark boundary excludes JSON construction, network transport,
//! strategy application, and persistence; the functional test separately runs
//! the real private owner and its generation-scoped delivery enrollment.

use super::*;
use futures_util::future::{pending, ready};

const SAMPLE_COUNT: usize = 100_000;
const BURST: usize = 64;
const WARMUP_COUNT: usize = 4_096;

fn lane_for_measurement() -> (
    PrivateApplyLane,
    crossbeam_channel::Receiver<PrivateApplyCommand>,
    crossbeam_channel::Receiver<PrivateApplyCommand>,
) {
    let (live_tx, live_rx) = crossbeam_channel::bounded(PRIVATE_APPLY_QUEUE_CAPACITY);
    let (replay_tx, replay_rx) = crossbeam_channel::bounded(PRIVATE_APPLY_QUEUE_CAPACITY);
    (
        PrivateApplyLane {
            live_tx,
            replay_tx,
            reconnect_generation: Arc::new(AtomicU64::new(0)),
            reconnect_notify: Arc::new(tokio::sync::Notify::new()),
        },
        live_rx,
        replay_rx,
    )
}

fn measured_event(sequence: usize) -> PrivateEventDelta {
    PrivateEventDelta::classify(serde_json::json!({
        "event_type": "order",
        "sequence": sequence,
    }))
    .unwrap()
}

fn assert_command_sequence(command: PrivateApplyCommand, sequence: usize, generation: u64) {
    match command {
        PrivateApplyCommand::Live {
            events,
            recovery_generation,
            ..
        } => {
            assert_eq!(recovery_generation, Some(generation));
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0].payload()["sequence"].as_u64(),
                Some(sequence as u64)
            );
        }
        _ => panic!("live frame was routed into a different command lane"),
    }
}

fn report_samples(
    scenario: &str,
    samples: &mut [u64],
    high_water: usize,
    audits: usize,
    first_warmup_ns: u64,
) {
    samples.sort_unstable();
    // Nearest-rank quantiles, with p expressed in thousandths.
    let percentile = |p: usize| samples[(samples.len() * p).div_ceil(1_000) - 1];
    eprintln!(
        "recovery_bench scenario={scenario} boundary=ready_classified_frame_select_to_live_lane_enqueue \
         n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} \
         lane_capacity={} queue_high_water={} overflow=0 fifo=true audit_completions={} \
         warmup={} first_warmup_dispatch_ns={} \
         backend=deterministic_future network_included=false owner_application_included=false",
        samples.len(),
        percentile(500),
        percentile(990),
        percentile(999),
        samples.last().copied().unwrap_or(0),
        PRIVATE_APPLY_QUEUE_CAPACITY,
        high_water,
        audits,
        WARMUP_COUNT,
        first_warmup_ns,
    );
}

async fn measure_selection_and_enqueue(scenario: &str, persistent_null: bool) {
    let (lane, live_rx, _replay_rx) = lane_for_measurement();
    let generation = 17;
    let mut samples = Vec::with_capacity(SAMPLE_COUNT);
    let mut high_water = 0;
    let mut drained = 0;
    let mut audit_completions = 0;
    let mut logical_now = Instant::now();
    let mut schedule = RecoveryAuditSchedule::new(logical_now);
    let mut first_warmup_ns = 0;

    // Quanta's lazy clock initialization can cost roughly 200 ms on first
    // use in an isolated test process. Keep that real observation separate
    // from steady-state distributions instead of silently dropping a maximum.
    for sequence in 0..WARMUP_COUNT {
        let events = vec![measured_event(sequence)];
        let started = Instant::now();
        match select_ws_or_recovery(ready(events), pending::<Result<(), ()>>()).await {
            RecoveryReadEvent::Socket(events) => {
                lane.dispatch_live(events, Some(generation)).unwrap();
            }
            RecoveryReadEvent::Audit(_) => panic!("pending audit unexpectedly completed"),
        }
        if sequence == 0 {
            first_warmup_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        }
        assert_command_sequence(live_rx.try_recv().unwrap(), sequence, generation);
    }

    for sequence in 0..SAMPLE_COUNT {
        // Drive actual production selection for the cold audit branch. Audit
        // latency is intentionally absent; null keeps scheduling bounded retry.
        if sequence % BURST == 0 && (persistent_null || sequence == 0) {
            assert!(schedule.ready(logical_now));
            let backend: Result<(), &'static str> = if persistent_null {
                Err("order lookup returned JSON null")
            } else {
                Ok(())
            };
            match select_ws_or_recovery(pending::<()>(), ready(backend)).await {
                RecoveryReadEvent::Audit(Err(_)) => {
                    let delay = schedule.failed(logical_now);
                    assert!(!schedule.ready(logical_now));
                    logical_now += delay;
                }
                RecoveryReadEvent::Audit(Ok(())) => schedule.succeeded(),
                RecoveryReadEvent::Socket(_) => panic!("pending socket unexpectedly completed"),
            }
            audit_completions += 1;
        }

        // Payload/Vec allocation is outside the measured boundary. The actual
        // production select and dispatch_live functions remain inside it.
        let events = vec![measured_event(sequence)];
        let started = Instant::now();
        match select_ws_or_recovery(ready(events), pending::<Result<(), ()>>()).await {
            RecoveryReadEvent::Socket(events) => {
                lane.dispatch_live(events, Some(generation)).unwrap();
            }
            RecoveryReadEvent::Audit(_) => panic!("pending audit unexpectedly completed"),
        }
        samples.push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        high_water = high_water.max(live_rx.len());

        if (sequence + 1) % BURST == 0 {
            while let Ok(command) = live_rx.try_recv() {
                assert_command_sequence(command, drained, generation);
                drained += 1;
            }
        }
    }
    while let Ok(command) = live_rx.try_recv() {
        assert_command_sequence(command, drained, generation);
        drained += 1;
    }
    assert_eq!(drained, SAMPLE_COUNT);
    assert_eq!(high_water, BURST);
    report_samples(
        scenario,
        &mut samples,
        high_water,
        audit_completions,
        first_warmup_ns,
    );
}

#[test]
fn focused_recovery_select_enqueue_benchmark() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        measure_selection_and_enqueue("normal_audit", false).await;
        measure_selection_and_enqueue("persistent_null_audit", true).await;
    });
}

#[test]
fn recovery_live_lane_saturation_preserves_fifo_and_reports_overflow() {
    let (lane, live_rx, _replay_rx) = lane_for_measurement();
    let generation = 31;
    for sequence in 0..PRIVATE_APPLY_QUEUE_CAPACITY {
        lane.dispatch_live(vec![measured_event(sequence)], Some(generation))
            .unwrap();
    }
    assert_eq!(live_rx.len(), PRIVATE_APPLY_QUEUE_CAPACITY);
    assert!(lane
        .dispatch_live(
            vec![measured_event(PRIVATE_APPLY_QUEUE_CAPACITY)],
            Some(generation),
        )
        .is_err());
    assert_eq!(live_rx.len(), PRIVATE_APPLY_QUEUE_CAPACITY);
    for sequence in 0..PRIVATE_APPLY_QUEUE_CAPACITY {
        assert_command_sequence(live_rx.try_recv().unwrap(), sequence, generation);
    }
    assert!(live_rx.is_empty());
    eprintln!(
        "recovery_bench scenario=saturated_live_lane capacity={} high_water={} overflow=1 accepted={} fifo=true overwrite=false",
        PRIVATE_APPLY_QUEUE_CAPACITY,
        PRIVATE_APPLY_QUEUE_CAPACITY,
        PRIVATE_APPLY_QUEUE_CAPACITY,
    );
}

#[test]
fn pending_and_null_audit_still_deliver_private_frame_without_opening_health_gate() {
    let shared = PolymarketTrade::new(
        "api-key",
        "c2VjcmV0",
        "passphrase",
        "0x0000000000000000000000000000000000000000000000000000000000000001",
        false,
        10,
        super::super::signer::SignatureType::Eoa,
    )
    .unwrap()
    .shared_state();
    shared.install_strategy_owner_routes(HashMap::from([("owner".to_string(), 0)]));
    shared.account_state.register_instance("owner", 1.0);
    shared
        .account_state
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    shared
        .account_state
        .reserve_order(
            "owner",
            "owner-1",
            "0xabc1",
            "TOKEN",
            Side::Buy,
            5.0,
            0.5,
            0,
        )
        .unwrap();
    shared.register_order_id("owner-1", "0xabc1", "TOKEN");
    let initial_reserved_cash = shared.account_state.order("owner-1").unwrap().reserved_cash;
    shared.user_feed_health.mark_strategy_consumer_ready();
    let generation = shared.user_feed_health.begin_recovery_delivery();
    let (updates_tx, updates_rx) = crossbeam_channel::bounded(4);
    let shutdown = Arc::new(AtomicBool::new(false));
    let (lane, workers) = spawn_private_apply_worker(
        Arc::clone(&shared),
        updates_tx.clone(),
        Arc::clone(&shutdown),
    )
    .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        // Substitute only the audit backend. Production OpenOrderRecovery::poll
        // retains this exact outstanding job while the actual read selector
        // continues to dispatch authenticated frames.
        let mut recovery = OpenOrderRecovery::new(generation);
        recovery.job = Some(tokio::spawn(pending::<Result<(), String>>()));
        let frame = Message::Text(
            serde_json::json!({
                "event_type": "order",
                "type": "PLACEMENT",
                "id": "0xabc1",
                "asset_id": "TOKEN",
                "side": "BUY",
                "price": "0.5",
                "original_size": "5",
                "size_matched": "0",
            })
            .to_string(),
        );
        let selected = tokio::time::timeout(
            Duration::from_millis(500),
            select_ws_or_recovery(
                ready(frame),
                recovery.poll(&shared, &updates_tx, &lane, &shutdown),
            ),
        )
        .await
        .expect("a pending audit must not prevent the production reader select from polling WS");
        let RecoveryReadEvent::Socket(Message::Text(text)) = selected else {
            panic!("private frame did not win over a pending audit");
        };
        let mut bytes = text.into_bytes();
        let payload: serde_json::Value = simd_json::serde::from_slice(&mut bytes).unwrap();
        let event = PrivateEventDelta::classify(payload).unwrap();
        lane.dispatch_live(vec![event], Some(generation)).unwrap();

        let update = updates_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("private owner must receive and route the live frame while audit is pending");
        assert_eq!(update.owner, 0);
        assert_eq!(update.update.client_order_id, "owner-1");
        assert_eq!(update.update.status, OrderStatus::Accepted);
        assert_eq!(
            shared
                .user_feed_health
                .recovery_delivery_progress(generation),
            Some((false, 1)),
            "live recovery delivery must be enrolled before the owning strategy can apply it",
        );
        assert!(shared.user_feed_health.is_recovering());

        recovery.job.take().unwrap().abort();
        for attempt in 1..=8 {
            recovery.job = Some(tokio::spawn(async { Err("JSON null".to_string()) }));
            while !recovery.job.as_ref().unwrap().is_finished() {
                tokio::task::yield_now().await;
            }
            // One real coordinator poll consumes the failed backend result,
            // retains Audit stage, and parks at its bounded retry deadline.
            // It must not launch another real HTTP job in this offline test.
            let mut audit = Box::pin(recovery.poll(&shared, &updates_tx, &lane, &shutdown));
            assert!(matches!(
                futures_util::poll!(audit.as_mut()),
                std::task::Poll::Pending
            ));
            drop(audit);
            assert_eq!(recovery.schedule.failures, attempt);
            assert!(matches!(recovery.stage, OpenOrderRecoveryStage::Audit));
            assert!(recovery.job.is_none());
            match select_ws_or_recovery(
                ready(()),
                recovery.poll(&shared, &updates_tx, &lane, &shutdown),
            )
            .await
            {
                RecoveryReadEvent::Socket(()) => {}
                RecoveryReadEvent::Audit(_) => {
                    panic!("failed audit must remain pending during retry")
                }
            }
            assert!(shared.user_feed_health.is_recovering());
            assert_eq!(
                shared
                    .user_feed_health
                    .recovery_delivery_progress(generation),
                Some((false, 1)),
            );
            assert_eq!(
                shared.account_state.order("owner-1").unwrap().reserved_cash,
                initial_reserved_cash,
                "an unavailable audit must not release the unknown order reservation",
            );
        }

        let (fence_tx, fence_rx) = tokio::sync::oneshot::channel();
        lane.live_tx
            .try_send(PrivateApplyCommand::RecoveryFence {
                generation,
                completion: fence_tx,
            })
            .unwrap_or_else(|_| panic!("recovery fence must fit behind drained live traffic"));
        assert!(tokio::time::timeout(Duration::from_secs(2), fence_rx)
            .await
            .unwrap()
            .unwrap());
        assert_eq!(
            shared
                .user_feed_health
                .recovery_delivery_progress(generation),
            Some((true, 1)),
            "FIFO fence closes enrollment but cannot acknowledge strategy application",
        );
        assert!(shared.user_feed_health.is_recovering());

        // Traffic accepted after the FIFO fence is ordinary live delivery and
        // must not attempt to register in the now closed recovery generation.
        let cancellation = PrivateEventDelta::classify(serde_json::json!({
            "event_type": "order",
            "type": "CANCELLATION",
            "id": "0xabc1",
            "asset_id": "TOKEN",
            "side": "BUY",
            "price": "0.5",
            "original_size": "5",
            "size_matched": "0",
        }))
        .unwrap();
        lane.dispatch_live(vec![cancellation], None).unwrap();
        let cancelled = updates_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(cancelled.owner, 0);
        assert_eq!(cancelled.update.status, OrderStatus::Cancelled);
        assert_eq!(
            shared
                .user_feed_health
                .recovery_delivery_progress(generation),
            Some((true, 1)),
        );

        let ack = shared
            .user_feed_health
            .recovery_update_ack("owner", &update.update)
            .expect("a real generation-scoped strategy application acknowledgement is required");
        drop(ack);
        assert_eq!(
            shared
                .user_feed_health
                .recovery_delivery_progress(generation),
            Some((true, 0)),
        );
        assert!(shared.user_feed_health.is_recovering());
    });

    shutdown.store(true, Ordering::Relaxed);
    drop(lane);
    for worker in workers {
        worker.join().unwrap();
    }
}
