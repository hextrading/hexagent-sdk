//! Offline, test-only physical owner publication benchmark. No exchange access.
use super::*;

#[test]
fn physical_fast_wait_wakes_for_health_and_preserves_command_fifo_and_disconnect() {
    use hexagent_runtime::{http1_pool::Role, poll_channel, wake::StateNotification};
    let wake = Arc::new(physical_owner_wake(Role::Fast));
    let hint = Arc::new(StateNotification::default());
    hint.bind(wake.clone());
    let (tx, rx) = poll_channel::bounded_with_wake(1, Some(wake));
    let (started, start) = bounded(1);
    let (done, receive) = bounded(1);
    let h = hint.clone();
    let worker = thread::spawn(move || {
        started.send(()).unwrap();
        assert!(matches!(rx.recv_timeout_observed_interruptible(
            Duration::from_secs(5), Duration::from_micros(50), &mut 0, || h.take_pending()),
            Err(crossbeam_channel::RecvTimeoutError::Timeout)));
        done.send(u64::MAX).unwrap();
        for expected in 0..64 {
            let value = loop {
                match rx.recv_timeout_observed_interruptible(
                    Duration::from_secs(2), Duration::from_micros(50), &mut 0, || h.take_pending()) {
                    Ok(value) => break value,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                    Err(error) => panic!("early disconnect: {error}"),
                }
            };
            assert_eq!(value, expected);
            done.send(value).unwrap();
        }
        assert!(matches!(rx.recv_timeout(Duration::from_secs(2)),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)));
    });
    start.recv().unwrap();
    thread::sleep(Duration::from_millis(10));
    hint.notify();
    assert_eq!(receive.recv_timeout(Duration::from_secs(1)).unwrap(), u64::MAX);
    for value in 0..64 {
        hint.notify();
        tx.try_send(value).unwrap();
        assert_eq!(receive.recv_timeout(Duration::from_secs(1)).unwrap(), value);
    }
    drop(tx);
    worker.join().unwrap();
}

#[test]
#[ignore = "release offline owner handoff A/B; writes raw samples only when requested"]
fn benchmark_http_owner_handoff() {
    use hexagent_runtime::http1_pool::Role;
    use std::time::Instant;
    const WARMUP: usize = 2_000;
    for parked in [false, true] {
        let n = if parked { 20_000 } else { 100_000 };
        let (tx, rx) = hexagent_runtime::poll_channel::bounded_with_wake(
            1,
            Some(Arc::new(hexagent_runtime::wake::Wake::default())),
        );
        let (diagnostic, diagnostics) = hexagent_runtime::poll_channel::bounded::<OwnerPublication>(512);
        let mut metrics = PolyConnectionLaneMetrics::new("bench", Role::Cancel, 1);
        metrics.publication = Some(diagnostic); // includes real telemetry publication
        let metrics = Arc::new(metrics);
        let lane = PolyConnectionLane {
            tx,
            metrics: metrics.clone(),
            preparation: None,
        };
        let (updates, _updates_rx) = bounded(1);
        let update_tx = ExecutorUpdateSender {
            owner: 1,
            tx: updates.into(),
        };
        let (done_tx, done_rx) = bounded(1);
        let mut local_rx = Some(rx);
        let worker = if parked {
            let rx = local_rx.take().unwrap();
            let metrics = metrics.clone();
            Some(thread::spawn(move || {
                for _ in 0..n + WARMUP {
                    let mut resumed = 0;
                    let mut command = rx
                        .recv_timeout_observed(
                            Duration::from_secs(2),
                            Duration::from_micros(50),
                            &mut resumed,
                        )
                        .unwrap();
                    let dequeued = crate::types::monotonic_now_ns();
                    let published = command.hot_path_mut().unwrap().owner_published_mono_ns;
                    metrics.release_for_test();
                    done_tx.send(dequeued - published).unwrap();
                }
            }))
        } else {
            None
        };
        let mut send = Vec::with_capacity(n);
        let mut receive = Vec::with_capacity(n);
        let mut diagnostic_records = 0;
        let mut diagnostic_high_water = 0;
        for i in 0..n + WARMUP {
            // Fixture construction is outside the production handoff boundary.
            let command = PolyConnectionCommand::Cancel {
                cancel_trigger: Default::default(),
                instance_id: "bench-instance".into(),
                exchange: Exchange::Polymarket,
                client_order_id: "bench-order".into(),
                timestamp_ns: 1,
                update_tx: update_tx.clone(),
                enqueued_at: Instant::now(),
            };
            let began = Instant::now();
            let mut pending = Some(command); // included in the same full-send timing boundary
            assert!(lane.try_send_slot(&mut pending).is_ok());
            let sent = began.elapsed().as_nanos() as u64;
            let elapsed = if let Some(rx) = &local_rx {
                let mut command = rx.try_recv().unwrap();
                let dequeued = crate::types::monotonic_now_ns();
                let published = command.hot_path_mut().unwrap().owner_published_mono_ns;
                metrics.release_for_test();
                dequeued - published
            } else {
                done_rx.recv_timeout(Duration::from_secs(2)).unwrap()
            };
            if i >= WARMUP {
                send.push(sent);
                receive.push(elapsed);
            }
            diagnostic_high_water = diagnostic_high_water.max(diagnostics.len());
            while diagnostics.try_recv().is_ok() {
                diagnostic_records += 1;
            }
        }
        if let Some(worker) = worker {
            worker.join().unwrap();
        }
        let mode = if parked { "parked" } else { "hot" };
        for (boundary, values) in [
            ("producer_try_send", &send),
            ("publication_to_dequeue", &receive),
        ] {
            let mut sorted = values.clone();
            sorted.sort_unstable();
            println!("owner_handoff mode={mode} boundary={boundary} n={n} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water={} overflow=0 diagnostic_high_water={diagnostic_high_water} diagnostic_records={diagnostic_records}",
                (sorted[n/2-1] + sorted[n/2])/2, sorted[(n*99).div_ceil(100)-1], sorted[(n*999).div_ceil(1000)-1], sorted[n-1],
                metrics.queue_high_water.load(Ordering::Relaxed));
            if let Ok(prefix) = std::env::var("HEX_HTTP_BENCH_OUT") {
                use std::io::Write;
                let mut f = std::io::BufWriter::new(
                    std::fs::File::create(format!("{prefix}-{mode}-{boundary}.csv")).unwrap(),
                );
                writeln!(f, "ns").unwrap();
                for value in values {
                    writeln!(f, "{value}").unwrap();
                }
            }
        }
        assert_eq!(metrics.queue_high_water.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.send_failures.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.diagnostic_dropped.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn full_diagnostic_lane_does_not_block_command_or_keep_cancel_occupied() {
    use hexagent_runtime::http1_pool::Role;
    let (diagnostic, diagnostics) = hexagent_runtime::poll_channel::bounded(1);
    diagnostic
        .try_send(ExecutionDiagnostic::OwnerSlow {
            account: Arc::from("fixture"),
            role: Role::Cancel,
            slot: 1,
            elapsed_ns: 0,
            connection: Default::default(),
        })
        .ok()
        .unwrap();
    let mut metrics = PolyConnectionLaneMetrics::new("fixture", Role::Cancel, 1);
    metrics.diagnostic = Some(diagnostic);
    let metrics = Arc::new(metrics);
    let (tx, rx) = hexagent_runtime::poll_channel::bounded(1);
    let lane = PolyConnectionLane {
        tx,
        metrics: metrics.clone(),
        preparation: None,
    };
    let (updates, _updates_rx) = bounded(1);
    let command = PolyConnectionCommand::Cancel {
        cancel_trigger: Default::default(),
        instance_id: "owner-a".into(),
        exchange: Exchange::Polymarket,
        client_order_id: "order-a".into(),
        timestamp_ns: 1,
        update_tx: ExecutorUpdateSender {
            owner: 1,
            tx: updates.into(),
        },
        enqueued_at: std::time::Instant::now(),
    };
    assert!(lane.try_send(command).is_ok());
    assert_eq!(metrics.diagnostic_dropped.load(Ordering::Relaxed), 1);
    assert!(matches!(
        rx.try_recv().unwrap(),
        PolyConnectionCommand::Cancel { .. }
    ));
    metrics.release(Duration::from_millis(600), Default::default());
    assert_eq!(metrics.diagnostic_dropped.load(Ordering::Relaxed), 2);
    assert!(!metrics.occupied.load(Ordering::Acquire));
    assert_eq!(metrics.cooldown_until_ns.load(Ordering::Relaxed), 0);
    assert_eq!(diagnostics.len(), 1);
}

#[test]
fn dedicated_slow_overflow_preserves_release_and_separates_publication_loss() {
    use hexagent_runtime::http1_pool::Role;
    let (tx, readers) = bounded(1);
    let registry = owner_diagnostics::Registry { tx, ready: Arc::new(AtomicBool::new(false)) };
    let mut metrics = PolyConnectionLaneMetrics::new("fixture", Role::Fast, 1);
    metrics.register_slow_diagnostic(&registry);
    let reader = readers.recv().unwrap();
    for _ in 0..=owner_diagnostics::CAPACITY {
        metrics.occupied.store(true, Ordering::Release);
        metrics.cancel_reply_pending.store(true, Ordering::Release);
        metrics.release(Duration::from_millis(501), Default::default());
        assert!(!metrics.occupied.load(Ordering::Acquire));
        assert!(!metrics.cancel_reply_pending.load(Ordering::Acquire));
        assert_eq!(metrics.cooldown_until_ns.load(Ordering::Relaxed), 0);
    }
    assert_eq!(metrics.diagnostic_dropped.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.publication_dropped.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.slow_diagnostic.as_ref().unwrap().stats().full, 1);
    drop(reader);
    metrics.release(Duration::from_millis(501), Default::default());
    assert_eq!(metrics.slow_diagnostic.as_ref().unwrap().stats().disconnected, 1);
    assert_eq!(metrics.diagnostic_dropped.load(Ordering::Relaxed), 2);
}

#[test]
fn coalesced_slow_hint_drains_all_owners_while_ordinary_channels_stay_open() {
    use hexagent_runtime::http1_pool::Role;
    use hexagent_runtime::poll_channel;
    let (diagnostic, diagnostic_rx) = poll_channel::bounded(16);
    let (publication, publication_rx) = poll_channel::bounded(16);
    let (worker, registry) = spawn_execution_diagnostics(diagnostic_rx, publication_rx).unwrap();
    let mut metrics = Vec::new();
    let mut writers = Vec::new();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    for slot in 0..8 {
        let mut m = PolyConnectionLaneMetrics::new("coalesced", Role::Fast, slot);
        m.register_slow_diagnostic(&registry);
        let m = Arc::new(m);
        metrics.push(m.clone());
        let barrier = barrier.clone();
        writers.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..owner_diagnostics::CAPACITY {
                m.release(Duration::from_millis(501), Default::default());
                thread::yield_now();
            }
        }));
    }
    drop(registry);
    for h in writers { h.join().unwrap(); }
    let deadline = Instant::now() + Duration::from_secs(2);
    while metrics.iter().any(|m| m.slow_diagnostic.as_ref().unwrap().stats().depth != 0) {
        assert!(Instant::now() < deadline, "coalescing must not strand records until shutdown");
        thread::sleep(Duration::from_millis(1));
    }
    for m in &metrics {
        assert_eq!(m.slow_requests.load(Ordering::Relaxed), owner_diagnostics::CAPACITY as u64);
        assert_eq!(m.diagnostic_dropped.load(Ordering::Relaxed), 0);
    }
    finish_execution_diagnostics(metrics, diagnostic, publication, worker);
}

#[test]
fn full_lane_evidence_diagnostic_preserves_health_and_exports_drop_count() {
    use hexagent_runtime::http1_pool::{
        BusinessHttpOutcome, BusinessHttpOutcomeSnapshot, PermitHealthSnapshot, Role,
    };
    let (diagnostic, diagnostics) = hexagent_runtime::poll_channel::bounded(1);
    diagnostic
        .try_send(ExecutionDiagnostic::OwnerSlow {
            account: Arc::from("fixture"),
            role: Role::Cancel,
            slot: 1,
            elapsed_ns: 0,
            connection: Default::default(),
        })
        .ok()
        .unwrap();
    let (mut publisher, receiver) = hexagent_runtime::latest_snapshot::channel();
    let now = now_ns();
    publisher.publish(LaneObservation {
        sequence: 1,
        observed_at_ns: now,
        observed_mono_ns: crate::types::monotonic_now_ns(),
        repair: None,
        health: PermitHealthSnapshot {
            pool_generation: 7,
            quarantined: false,
        },
        business: Some(BusinessHttpOutcomeSnapshot {
            attempt_id: 1,
            pool_generation: 7,
            elapsed_ns: 10,
            outcome: BusinessHttpOutcome::Healthy,
            status_code: 200,
        }),
        busy: false,
        cumulative_failures: 0,
        cumulative_slow: 0,
        cumulative_no_response: 0,
        cumulative_slow_retired: 0,
        slow_retired_generation: 0,
        no_response_generation: 0,
        no_response_peer: None,
    });
    let mut routes = PolyAccountConnectionRoutes {
        health: Some(AccountExecutionAdmission::new(0, 2, now)),
        health_diagnostic: Some(diagnostic),
        health_lanes: vec![(Role::Cancel, 1, receiver)],
        ..Default::default()
    };
    routes.refresh_health(now_ns());
    assert_eq!(routes.lane_evidence_dropped, 1);
    assert_eq!(
        routes
            .health
            .as_ref()
            .unwrap()
            .cancel_lane_rank(1, now_ns()),
        0
    );
    diagnostics.try_recv().unwrap();
    routes.admission_last_diagnostic_ns = 0;
    routes.refresh_health(now_ns());
    assert!(matches!(
        diagnostics.try_recv().unwrap(),
        ExecutionDiagnostic::Admission {
            lane_evidence_dropped: 1,
            ..
        }
    ));
}

// Test-only 2x2 controls: preserve the previous slot producer exactly except
// the selected diagnostic publication method. No switch enters production.
#[inline(always)]
fn publish_payload_diagnostic<const OWNED: bool>(sender: &hexagent_runtime::poll_channel::Sender<ExecutionDiagnostic>, record: ExecutionDiagnostic) -> Result<(), crossbeam_channel::TrySendError<ExecutionDiagnostic>> {
    if OWNED { sender.try_send_owned(record) } else { sender.try_send(record) }
}
fn previous_payload_send<const OWNED_DIAGNOSTIC: bool>(lane: &PolyConnectionLane, mut command: PolyConnectionCommand) -> Result<(), crossbeam_channel::TrySendError<PolyConnectionCommand>> {
        if lane
            .metrics
            .occupied
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            lane.metrics.busy_skips.fetch_add(1, Ordering::Relaxed);
            return Err(crossbeam_channel::TrySendError::Full(command));
        }
        lane.metrics.enqueued_ns.store(now_ns(), Ordering::Release);
        lane.metrics.cancel_reply_pending.store(
            matches!(command, PolyConnectionCommand::Cancel { .. }), Ordering::Release);
        let prepares = matches!(&command, PolyConnectionCommand::Place { .. });
        let has_trace = command.hot_path_mut().is_some();
        lane.metrics.queue_depth.store(1, Ordering::Release);
        let mut pending = Some(command);
        let sent = lane.tx.try_send_observed_slot(&mut pending, |command, started| {
            if let Some(trace) = command.hot_path_mut() { trace.owner_published_mono_ns = started; }
        });
        match sent {
            Ok(timing) => {
                if prepares { if let Some(route) = &lane.preparation { route.issued(); } }
                if has_trace {
                    let publication_mono_ns = timing.push_started_mono_ns;
                    if let Some(sender) = &lane.metrics.diagnostic {
                        if publish_payload_diagnostic::<OWNED_DIAGNOSTIC>(sender, ExecutionDiagnostic::OwnerPublication {
                            account: Arc::clone(&lane.metrics.account_id), role: lane.metrics.role,
                            slot: lane.metrics.slot, publication_mono_ns, timing }).is_err() {
                            lane.metrics.diagnostic_dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                // occupied admits at most one command; this gauge only ever
                // transitions 0 -> 1. No read/modify/write is needed per send.
                lane.metrics.queue_high_water.store(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                lane.metrics.cancel_reply_pending.store(false, Ordering::Release);
                lane.metrics.send_failures.fetch_add(1, Ordering::Relaxed);
                lane.metrics.enqueued_ns.store(0, Ordering::Release);
                lane.metrics.queue_depth.store(0, Ordering::Release);
                lane.metrics.occupied.store(false, Ordering::Release);
                let command = pending.take().expect("failed publication retains the command");
                Err(match error {
                    hexagent_runtime::poll_channel::SlotSendError::Disconnected => crossbeam_channel::TrySendError::Disconnected(command),
                    _ => crossbeam_channel::TrySendError::Full(command),
                })
            }
        }
    }
fn owned_payload_send<const OWNED_DIAGNOSTIC: bool>(lane: &PolyConnectionLane, command: PolyConnectionCommand) -> Result<(), crossbeam_channel::TrySendError<PolyConnectionCommand>> {
        if lane
            .metrics
            .occupied
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            lane.metrics.busy_skips.fetch_add(1, Ordering::Relaxed);
            return Err(crossbeam_channel::TrySendError::Full(command));
        }
        lane.metrics.enqueued_ns.store(now_ns(), Ordering::Release);
        lane.metrics.cancel_reply_pending.store(
            matches!(command, PolyConnectionCommand::Cancel { .. }), Ordering::Release);
        let prepares = matches!(&command, PolyConnectionCommand::Place { .. });
        let has_trace = matches!(&command, PolyConnectionCommand::Place { .. } | PolyConnectionCommand::Cancel { .. });
        lane.metrics.queue_depth.store(1, Ordering::Release);
        let sent = lane.tx.try_send_stamped(command);
        match sent {
            Ok(timing) => {
                if prepares { if let Some(route) = &lane.preparation { route.issued(); } }
                if has_trace {
                    let publication_mono_ns = timing.push_started_mono_ns;
                    if let Some(sender) = &lane.metrics.diagnostic {
                        if publish_payload_diagnostic::<OWNED_DIAGNOSTIC>(sender, ExecutionDiagnostic::OwnerPublication {
                            account: Arc::clone(&lane.metrics.account_id), role: lane.metrics.role,
                            slot: lane.metrics.slot, publication_mono_ns, timing }).is_err() {
                            lane.metrics.diagnostic_dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                // occupied admits at most one command; this gauge only ever
                // transitions 0 -> 1. No read/modify/write is needed per send.
                lane.metrics.queue_high_water.store(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                lane.metrics.cancel_reply_pending.store(false, Ordering::Release);
                lane.metrics.send_failures.fetch_add(1, Ordering::Relaxed);
                lane.metrics.enqueued_ns.store(0, Ordering::Release);
                lane.metrics.queue_depth.store(0, Ordering::Release);
                lane.metrics.occupied.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

// Test-only frozen v1 producer for same-binary ablations. Commands and clocks
// match production; CLOCKS/DIAGNOSTIC change only the added observation work.
fn legacy_observed_send<const CLOCKS: bool, const DIAGNOSTIC: bool>(
    lane: &PolyConnectionLane,
    mut command: PolyConnectionCommand,
) -> Result<(), crossbeam_channel::TrySendError<PolyConnectionCommand>> {
    if lane
        .metrics
        .occupied
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        lane.metrics.busy_skips.fetch_add(1, Ordering::Relaxed);
        return Err(crossbeam_channel::TrySendError::Full(command));
    }
    lane.metrics.enqueued_ns.store(now_ns(), Ordering::Release);
    lane.metrics.cancel_reply_pending.store(
        matches!(command, PolyConnectionCommand::Cancel { .. }),
        Ordering::Release,
    );
    let prepares = matches!(&command, PolyConnectionCommand::Place { .. });
    let publication_mono_ns = command
        .hot_path_mut()
        .map(|trace| {
            trace.owner_published_mono_ns = crate::types::monotonic_now_ns();
            trace.owner_published_mono_ns
        })
        .unwrap_or(0);
    lane.metrics.queue_depth.store(1, Ordering::Release);
    let result = if CLOCKS {
        lane.tx.try_send_observed(command)
    } else {
        lane.tx.try_send(command).map(|_| Default::default())
    };
    match result {
        Ok(timing) => {
            if prepares {
                if let Some(route) = &lane.preparation {
                    route.issued();
                }
            }
            if DIAGNOSTIC && publication_mono_ns != 0 {
                if let Some(sender) = &lane.metrics.diagnostic {
                    if sender
                        .try_send(ExecutionDiagnostic::OwnerPublication {
                            account: Arc::clone(&lane.metrics.account_id),
                            role: lane.metrics.role,
                            slot: lane.metrics.slot,
                            publication_mono_ns,
                            timing,
                        })
                        .is_err()
                    {
                        lane.metrics
                            .diagnostic_dropped
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            lane.metrics
                .queue_high_water
                .fetch_max(1, Ordering::Relaxed);
            Ok(())
        }
        Err(error) => {
            lane.metrics
                .cancel_reply_pending
                .store(false, Ordering::Release);
            lane.metrics.send_failures.fetch_add(1, Ordering::Relaxed);
            lane.metrics.enqueued_ns.store(0, Ordering::Release);
            lane.metrics.queue_depth.store(0, Ordering::Release);
            lane.metrics.occupied.store(false, Ordering::Release);
            Err(error)
        }
    }
}

#[test]
#[ignore = "offline same-binary clock/diagnostic ablation with batch CPU accounting"]
fn benchmark_owner_observation_ablation() {
    use hexagent_runtime::{http1_pool::Role, poll_channel};
    use std::io::Write;
    const BLOCK: usize = 128;
    const BATCHES: usize = 1024;
    const VARIANTS: usize = 9;
    let labels = [
        "base",
        "clocks",
        "diagnostic",
        "legacy_full",
        "production",
        "raw_clocks",
        "previous",
        "owned_command",
        "owned_diagnostic",
    ];
    struct Fixture {
        lane: PolyConnectionLane,
        receiver: poll_channel::Receiver<PolyConnectionCommand>,
        diagnostics: poll_channel::Receiver<ExecutionDiagnostic>,
        command: Option<PolyConnectionCommand>,
    }
    let (updates, _updates_rx) = bounded(1);
    let mut fixtures = (0..VARIANTS)
        .map(|_| {
            let (tx, receiver) = poll_channel::bounded_with_wake(
                1,
                Some(Arc::new(hexagent_runtime::wake::Wake::default())),
            );
            let (diagnostic, diagnostics) = poll_channel::bounded(EXECUTION_DIAGNOSTIC_CAPACITY);
            let mut metrics = PolyConnectionLaneMetrics::new("bench", Role::Cancel, 1);
            metrics.diagnostic = Some(diagnostic);
            Fixture {
                lane: PolyConnectionLane {
                    tx,
                    metrics: Arc::new(metrics),
                    preparation: None,
                },
                receiver,
                diagnostics,
                command: Some(PolyConnectionCommand::Cancel {
                    cancel_trigger: Default::default(),
                    instance_id: "bench-instance".into(),
                    exchange: Exchange::Polymarket,
                    client_order_id: "bench-order".into(),
                    timestamp_ns: 1,
                    update_tx: ExecutorUpdateSender {
                        owner: 1,
                        tx: updates.clone().into(),
                    },
                    enqueued_at: std::time::Instant::now(),
                }),
            }
        })
        .collect::<Vec<_>>();
    let mut samples = (0..VARIANTS)
        .map(|_| Vec::<(u64, u64)>::with_capacity(BATCHES * BLOCK))
        .collect::<Vec<_>>();
    let mut batches = Vec::with_capacity(VARIANTS * BATCHES);
    println!("ablation command_bytes={} diagnostic_bytes={} timing_bytes={} cpu_clock=thread_cpu_ns batch={} wake_backend={}",
        std::mem::size_of::<PolyConnectionCommand>(), std::mem::size_of::<ExecutionDiagnostic>(),
        std::mem::size_of::<poll_channel::PublicationTiming>(), BLOCK,
        if cfg!(target_os="linux") {"futex"} else {"sleep_poll"});
    for batch in 0..BATCHES + 16 {
        // Rotate every block, not just process order, to share scheduling noise.
        for offset in 0..VARIANTS {
            let variant = (batch + offset) % VARIANTS;
            let f = &mut fixtures[variant];
            let cpu_before = hexagent_runtime::latency::thread_cpu_ns();
            let wall_before = std::time::Instant::now();
            let mut block_max = 0;
            let mut tails = 0;
            for _ in 0..BLOCK {
                let command = f.command.take().unwrap();
                let started = std::time::Instant::now();
                let result = match variant {
                    0 => legacy_observed_send::<false, false>(&f.lane, command),
                    1 => legacy_observed_send::<true, false>(&f.lane, command),
                    2 => legacy_observed_send::<false, true>(&f.lane, command),
                    3 => legacy_observed_send::<true, true>(&f.lane, command),
                    5 => {
                        for _ in 0..3 {
                            std::hint::black_box(crate::types::monotonic_now_ns());
                        }
                        legacy_observed_send::<false, false>(&f.lane, command)
                    }
                    6 => previous_payload_send::<false>(&f.lane, command),
                    7 => owned_payload_send::<false>(&f.lane, command),
                    8 => previous_payload_send::<true>(&f.lane, command),
                    _ => f.lane.try_send(command),
                };
                let elapsed = started.elapsed().as_nanos() as u64;
                assert!(result.is_ok());
                let mut command = f.receiver.try_recv().unwrap();
                let dequeued = crate::types::monotonic_now_ns();
                let published = command.hot_path_mut().unwrap().owner_published_mono_ns;
                f.lane.metrics.release_for_test();
                f.command = Some(command);
                if batch >= 16 {
                    samples[variant].push((elapsed, dequeued - published));
                }
                block_max = block_max.max(elapsed);
                tails += usize::from(elapsed >= 5_000);
                while f.diagnostics.try_recv().is_ok() {}
            }
            let wall = wall_before.elapsed().as_nanos() as u64;
            let cpu_after = hexagent_runtime::latency::thread_cpu_ns();
            assert!(
                cpu_after >= cpu_before && cpu_before > 0,
                "CPU clock unavailable"
            );
            // CPU bracket encloses the wall bracket: off-CPU is a conservative
            // batch lower bound, not a claimed per-event scheduling timestamp.
            if batch >= 16 {
                batches.push((
                    variant,
                    batch - 16,
                    wall,
                    cpu_after - cpu_before,
                    block_max,
                    tails,
                ));
            }
        }
    }
    let prefix = std::env::var("HEX_HTTP_BENCH_OUT").expect("raw evidence destination required");
    for variant in 0..VARIANTS {
        let mut out = std::io::BufWriter::new(
            std::fs::File::create(format!("{prefix}-{}.csv", labels[variant])).unwrap(),
        );
        writeln!(out, "send_ns,handoff_ns").unwrap();
        for (send, handoff) in &samples[variant] {
            writeln!(out, "{send},{handoff}").unwrap();
        }
        assert_eq!(
            fixtures[variant]
                .lane
                .metrics
                .send_failures
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            fixtures[variant]
                .lane
                .metrics
                .diagnostic_dropped
                .load(Ordering::Relaxed),
            0
        );
        println!(
            "ablation variant={} n={} queue_high_water=1 overflow=0 diagnostic_dropped=0",
            labels[variant],
            samples[variant].len()
        );
    }
    let mut out =
        std::io::BufWriter::new(std::fs::File::create(format!("{prefix}-batches.csv")).unwrap());
    writeln!(
        out,
        "variant,batch,wall_ns,cpu_ns,off_cpu_lower_ns,max_send_ns,send_over_5us"
    )
    .unwrap();
    for (v, b, w, c, m, t) in batches {
        writeln!(
            out,
            "{},{b},{w},{c},{},{m},{t}",
            labels[v],
            w.saturating_sub(c)
        )
        .unwrap();
    }
}

#[test]
fn diagnostics_shutdown_releases_metrics_sender_clones_before_join() {
    let (tx, rx) = hexagent_runtime::poll_channel::bounded(8);
    let (publications, published) = hexagent_runtime::poll_channel::bounded(8);
    let (handle, registration) = spawn_execution_diagnostics(rx, published).unwrap();
    let mut metrics =
        PolyConnectionLaneMetrics::new("fixture", hexagent_runtime::http1_pool::Role::Cancel, 1);
    metrics.diagnostic = Some(tx.clone());
    metrics.publication = Some(publications.clone());
    metrics.register_slow_diagnostic(&registration);
    metrics.release(Duration::from_millis(501), Default::default());
    drop(registration);
    let (done_tx, done_rx) = bounded(1);
    let shutdown = thread::spawn(move || {
        finish_execution_diagnostics(vec![Arc::new(metrics)], tx, publications, handle);
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("diagnostic worker must observe disconnect");
    shutdown.join().unwrap();
}

#[test]
fn owned_publication_keeps_fifo_fields_and_exact_arc_ownership() {
    use hexagent_runtime::http1_pool::Role;
    let account: Arc<str> = Arc::from("fixture");
    let (tx, rx) = hexagent_runtime::poll_channel::bounded(2);
    for slot in 1..=2 {
        tx.try_send_owned(OwnerPublication {
            account: account.clone(), role: Role::Cancel, slot,
            timing: hexagent_runtime::poll_channel::PublicationTiming {
                push_started_mono_ns: slot as u64 * 10,
                push_returned_mono_ns: slot as u64 * 10 + 1,
                notify_returned_mono_ns: slot as u64 * 10 + 2,
            },
        }).unwrap();
    }
    assert_eq!(Arc::strong_count(&account), 3);
    for slot in 1..=2 {
        let p = rx.try_recv().unwrap();
        assert!(Arc::ptr_eq(&p.account, &account));
        assert_eq!((p.role, p.slot), (Role::Cancel, slot));
        assert_eq!(p.timing.push_started_mono_ns, slot as u64 * 10);
        assert_eq!(p.timing.push_returned_mono_ns, slot as u64 * 10 + 1);
        assert_eq!(p.timing.notify_returned_mono_ns, slot as u64 * 10 + 2);
    }
    assert_eq!(Arc::strong_count(&account), 1);
    assert!(rx.try_recv().is_err());
    drop(rx);
    let retained = tx.try_send_owned(OwnerPublication {
        account: account.clone(), role: Role::Fast, slot: 9,
        timing: hexagent_runtime::poll_channel::PublicationTiming::default(),
    }).unwrap_err().into_inner();
    assert_eq!(retained.slot, 9);
    assert_eq!(Arc::strong_count(&account), 2);
    drop(retained);
    assert_eq!(Arc::strong_count(&account), 1);
}

#[test]
fn publication_lane_full_preserves_command_and_separates_health_evidence() {
    use hexagent_runtime::http1_pool::Role;
    let (pub_tx, pub_rx) = hexagent_runtime::poll_channel::bounded(1);
    let (diag_tx, diag_rx) = hexagent_runtime::poll_channel::bounded(1);
    assert!(std::mem::size_of::<OwnerPublication>() <= 64);
    let timing = hexagent_runtime::poll_channel::PublicationTiming {
        push_started_mono_ns: 11, push_returned_mono_ns: 22, notify_returned_mono_ns: 33,
    };
    pub_tx.try_send(OwnerPublication { account: Arc::from("prior"), role: Role::Fast,
        slot: 7, timing }).unwrap();
    let mut metrics = PolyConnectionLaneMetrics::new("next", Role::Cancel, 2);
    metrics.publication = Some(pub_tx);
    metrics.diagnostic = Some(diag_tx);
    let metrics = Arc::new(metrics);
    let (tx, rx) = hexagent_runtime::poll_channel::bounded(1);
    let lane = PolyConnectionLane { tx, metrics: metrics.clone(), preparation: None };
    let (updates, _rx) = bounded(1);
    lane.try_send(PolyConnectionCommand::Cancel { cancel_trigger: Default::default(),
        instance_id: "instance".into(), exchange: Exchange::Polymarket,
        client_order_id: "order".into(), timestamp_ns: 1,
        update_tx: ExecutorUpdateSender { owner: 7, tx: updates.into() },
        enqueued_at: Instant::now() }).unwrap();
    assert_eq!(metrics.diagnostic_dropped.load(Ordering::Relaxed), 1);
    assert!(matches!(rx.try_recv().unwrap(), PolyConnectionCommand::Cancel { client_order_id, .. } if client_order_id == "order"));
    metrics.release(Duration::from_millis(600), Default::default());
    assert!(!metrics.occupied.load(Ordering::Acquire));
    assert!(matches!(diag_rx.try_recv().unwrap(), ExecutionDiagnostic::OwnerSlow { slot: 2, .. }));
    let record = pub_rx.try_recv().unwrap();
    assert_eq!((record.account.as_ref(), record.role, record.slot, record.timing), ("prior", Role::Fast, 7, timing));
    assert_eq!(metrics.diagnostic_dropped.load(Ordering::Relaxed), 1);
    assert!(pub_rx.try_recv().is_err());
}

#[test]
#[ignore = "intrusive receive-span CPU probe; separate from acceptance latency A/B"]
fn benchmark_owner_scheduler_probe() {
    use hexagent_runtime::{http1_pool::Role, poll_channel};
    use std::io::Write;
    const N: usize = 20_000;
    let (tx, rx) =
        poll_channel::bounded_with_wake(1, Some(Arc::new(hexagent_runtime::wake::Wake::default())));
    let (diagnostic, diagnostics) = poll_channel::bounded(1024);
    let mut metrics = PolyConnectionLaneMetrics::new("probe", Role::Cancel, 1);
    metrics.diagnostic = Some(diagnostic);
    let metrics = Arc::new(metrics);
    let lane = PolyConnectionLane {
        tx,
        metrics: metrics.clone(),
        preparation: None,
    };
    let (done_tx, done_rx) = bounded(1);
    let worker = thread::spawn(move || {
        for _ in 0..N {
            let cpu_before = hexagent_runtime::latency::thread_cpu_ns();
            let wait_started = crate::types::monotonic_now_ns();
            let mut resumed = 0;
            let mut command = rx
                .recv_timeout_observed(
                    Duration::from_secs(2),
                    Duration::from_micros(50),
                    &mut resumed,
                )
                .unwrap();
            let dequeued = crate::types::monotonic_now_ns();
            let cpu_after = hexagent_runtime::latency::thread_cpu_ns();
            let published = command.hot_path_mut().unwrap().owner_published_mono_ns;
            metrics.release_for_test();
            assert!(cpu_before > 0 && cpu_after >= cpu_before);
            done_tx
                .send((
                    wait_started,
                    resumed,
                    dequeued,
                    published,
                    cpu_after - cpu_before,
                ))
                .unwrap();
        }
    });
    let (updates, _updates_rx) = bounded(1);
    let mut rows = Vec::with_capacity(N);
    for _ in 0..N {
        let command = PolyConnectionCommand::Cancel {
            cancel_trigger: Default::default(),
            instance_id: "probe".into(),
            exchange: Exchange::Polymarket,
            client_order_id: "probe".into(),
            timestamp_ns: 1,
            update_tx: ExecutorUpdateSender {
                owner: 1,
                tx: updates.clone().into(),
            },
            enqueued_at: std::time::Instant::now(),
        };
        assert!(lane.try_send(command).is_ok());
        let (start, resume, dequeue, published, cpu) =
            done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let record = diagnostics.try_recv().unwrap();
        let ExecutionDiagnostic::OwnerPublication {
            publication_mono_ns,
            timing,
            ..
        } = record
        else {
            panic!("publication required")
        };
        assert_eq!(publication_mono_ns, published);
        rows.push((
            dequeue - published,
            dequeue - start,
            cpu,
            resume,
            dequeue,
            timing.notify_returned_mono_ns,
        ));
    }
    worker.join().unwrap();
    let prefix = std::env::var("HEX_HTTP_BENCH_OUT").unwrap();
    let mut out =
        std::io::BufWriter::new(std::fs::File::create(format!("{prefix}-scheduler.csv")).unwrap());
    writeln!(out,"handoff_ns,receive_span_wall_ns,receive_span_cpu_ns,receive_span_off_cpu_lower_ns,resumed,userspace_resume_to_dequeue_ns,notify_return_to_resume_signed_ns").unwrap();
    for (handoff, wall, cpu, resume, dequeue, notify) in rows {
        writeln!(
            out,
            "{handoff},{wall},{cpu},{},{},{},{}",
            wall.saturating_sub(cpu),
            u8::from(resume != 0),
            if resume == 0 {
                String::new()
            } else {
                (dequeue - resume).to_string()
            },
            if resume == 0 {
                String::new()
            } else {
                (resume as i128 - notify as i128).to_string()
            }
        )
        .unwrap();
    }
    assert_eq!(lane.metrics.diagnostic_dropped.load(Ordering::Relaxed), 0);
    println!("scheduler_probe n={N} business_overflow=0 diagnostic_dropped=0 queue_high_water=1 boundary=whole_receive_span_including_prepublication_idle cpu_probe_intrusive=true");
}

#[test]
#[ignore = "offline layout evidence for command and diagnostic publication"]
fn owner_payload_layout_probe() {
    println!("owner_layout command={} order={} cancel_trigger={} signal={} probe={} diagnostic={} timing={}",
        std::mem::size_of::<PolyConnectionCommand>(), std::mem::size_of::<OrderRequest>(),
        std::mem::size_of::<CancelTrigger>(), std::mem::size_of::<Signal>(),
        std::mem::size_of::<ProbeHttpRequest>(), std::mem::size_of::<ExecutionDiagnostic>(),
        std::mem::size_of::<hexagent_runtime::poll_channel::PublicationTiming>());
}

// Frozen starting implementation for same-binary ownership-transfer ablation.
impl PolyConnectionLane {
    fn legacy_burst_try_send(
        &self,
        command: PolyConnectionCommand,
    ) -> Result<(), crossbeam_channel::TrySendError<PolyConnectionCommand>> {
        if self
            .metrics
            .occupied
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.metrics.busy_skips.fetch_add(1, Ordering::Relaxed);
            return Err(crossbeam_channel::TrySendError::Full(command));
        }
        self.metrics.enqueued_ns.store(now_ns(), Ordering::Release);
        self.metrics.cancel_reply_pending.store(
            matches!(command, PolyConnectionCommand::Cancel { .. }), Ordering::Release);
        let prepares = matches!(&command, PolyConnectionCommand::Place { .. });
        let has_trace = matches!(&command, PolyConnectionCommand::Place { .. } | PolyConnectionCommand::Cancel { .. });
        self.metrics.queue_depth.store(1, Ordering::Release);
        let sent = self.tx.try_send_stamped(command);
        match sent {
            Ok(timing) => {
                if prepares { if let Some(route) = &self.preparation { route.issued(); } }
                if has_trace {
                    let publication_mono_ns = timing.push_started_mono_ns;
                    if let Some(sender) = &self.metrics.diagnostic {
                        if sender.try_send_owned(ExecutionDiagnostic::OwnerPublication {
                            account: Arc::clone(&self.metrics.account_id), role: self.metrics.role,
                            slot: self.metrics.slot, publication_mono_ns, timing }).is_err() {
                            self.metrics.diagnostic_dropped.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                // occupied admits at most one command; this gauge only ever
                // transitions 0 -> 1. No read/modify/write is needed per send.
                self.metrics.queue_high_water.store(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.metrics.cancel_reply_pending.store(false, Ordering::Release);
                self.metrics.send_failures.fetch_add(1, Ordering::Relaxed);
                self.metrics.enqueued_ns.store(0, Ordering::Release);
                self.metrics.queue_depth.store(0, Ordering::Release);
                self.metrics.occupied.store(false, Ordering::Release);
                Err(error)
            }
        }
    }

}
fn legacy_burst_route(
    routes: &mut PolyAccountConnectionRoutes,
    role: hexagent_runtime::http1_pool::Role,
    mut command: PolyConnectionCommand,
) -> Result<(), PolyConnectionCommand> {
    // Production initializes these alongside the lanes before dispatch starts.
    #[cfg(test)]
    routes.cancel_lane_keys.resize(routes.cancel.len(), None);
    let cancel_key = command.cancel_key();
    let now = now_ns();
    routes.refresh_health_if_needed(now);
    let now = now_ns();
    let PolyAccountConnectionRoutes { fast, cancel, reconcile, fast_rr, cancel_rr,
        reconcile_rr, health, cancel_lane_keys, maintenance_pending, .. } = routes;
    let (lanes, rr) = match role {
        hexagent_runtime::http1_pool::Role::Fast => (&*fast, fast_rr),
        hexagent_runtime::http1_pool::Role::Cancel => (&*cancel, cancel_rr),
        hexagent_runtime::http1_pool::Role::Reconcile => (&*reconcile, reconcile_rr),
        _ => return Err(command),
    };
    if lanes.is_empty() {
        return Err(command);
    }
    let mut start = *rr % lanes.len();
    let mut selection = crate::types::FastLaneSelection::default();
    if role == hexagent_runtime::http1_pool::Role::Fast {
        if let Some(route) = lanes.first().and_then(|lane| lane.preparation.as_ref()) { route.refresh(); }
        // Rank only healthy idle candidates. Equal CPU load retains RR fairness.
        let mut best = u64::MAX;
        for offset in 0..lanes.len() {
            let index = (*rr + offset) % lanes.len();
            let lane = &lanes[index];
            if lane.metrics.occupied.load(Ordering::Acquire) {
                selection.busy_lanes = selection.busy_lanes.saturating_add(1);
                continue;
            }
            if health.as_mut().is_some_and(|health| !health.lane_place_allowed(lane.metrics.slot, now)) {
                selection.unhealthy_lanes = selection.unhealthy_lanes.saturating_add(1);
                continue;
            }
            selection.eligible_lanes = selection.eligible_lanes.saturating_add(1);
            let score = lane.preparation.as_ref().map_or(0, |route| route.pending_on_core());
            if score < best { best = score; start = index; }
        }
        selection.minimum_pending = (best != u64::MAX).then_some(best);
    }
    let passes = if role == hexagent_runtime::http1_pool::Role::Cancel && health.is_some() { 3 } else { 1 };
    for rank in 0..passes {
        for offset in 0..lanes.len() {
            let index = (start + offset) % lanes.len();
            if role == hexagent_runtime::http1_pool::Role::Cancel
                && health.as_ref().is_some_and(|h| h.cancel_lane_rank(lanes[index].metrics.slot, now) != rank) {
                continue;
            }
            if matches!(&command, PolyConnectionCommand::ProbeHttp { request, .. }
                if request.excluded_slot == Some(lanes[index].metrics.slot)) {
                continue;
            }
            if role == hexagent_runtime::http1_pool::Role::Fast
                && health.as_mut().is_some_and(|health| !health.lane_place_allowed(lanes[index].metrics.slot, now))
            {
                continue;
            }
            // Fast/Cancel health has one generation-aware router authority.
            if role == hexagent_runtime::http1_pool::Role::Reconcile
                && lanes[index]
                .metrics
                .cooldown_until_ns
                .load(Ordering::Acquire)
                > now
            {
                lanes[index]
                    .metrics
                    .cooldown_skips
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            match &mut command {
                PolyConnectionCommand::Place { expected_generation, .. }
                | PolyConnectionCommand::ProbeHttp { expected_generation, .. } if role == hexagent_runtime::http1_pool::Role::Fast => {
                    *expected_generation = health.as_ref().and_then(|health| health.lane_generation(lanes[index].metrics.slot));
                }
                _ => {}
            }
            if role == hexagent_runtime::http1_pool::Role::Fast {
                if let PolyConnectionCommand::Place { order, .. } = &mut command {
                    order.hot_path.fast_selection = lanes[index].preparation.as_ref().map(|route| {
                        selection.core = route.core() as u32;
                        selection.selected_pending = route.pending_on_core();
                        selection
                    });
                }
            }
            match lanes[index].legacy_burst_try_send(command) {
                Ok(()) => {
                    *maintenance_pending = true;
                    *rr = index.wrapping_add(1);
                    if role == hexagent_runtime::http1_pool::Role::Cancel {
                        cancel_lane_keys[index] = cancel_key;
                    }
                    if role == hexagent_runtime::http1_pool::Role::Fast {
                        if let Some(health) = health.as_mut() {
                            health.place_dispatched(lanes[index].metrics.slot, now_ns());
                        }
                    }
                    return Ok(());
                }
                Err(crossbeam_channel::TrySendError::Full(returned)) => command = returned,
                Err(crossbeam_channel::TrySendError::Disconnected(returned)) => command = returned,
            }
        }
    }
    // Reconcile work is lossless. A cooling lane remains a bounded
    // last resort when every healthy connection is occupied; no request is
    // dropped and no extra socket is created. Fast/place never falls through
    // to a known-slow generation.
    if matches!(
        role,
        hexagent_runtime::http1_pool::Role::Reconcile
    ) {
        for offset in 0..lanes.len() {
            let index = (start + offset) % lanes.len();
        if matches!(&command, PolyConnectionCommand::ProbeHttp { request, .. }
            if request.excluded_slot == Some(lanes[index].metrics.slot)) {
            continue;
        }
            if lanes[index]
                .metrics
                .cooldown_until_ns
                .load(Ordering::Acquire)
                <= now
            {
                continue;
            }
            match lanes[index].legacy_burst_try_send(command) {
                Ok(()) => {
                    *rr = index.wrapping_add(1);
                    if role == hexagent_runtime::http1_pool::Role::Cancel {
                        cancel_lane_keys[index] = cancel_key;
                    }
                    return Ok(());
                }
                Err(crossbeam_channel::TrySendError::Full(returned)) => command = returned,
                Err(crossbeam_channel::TrySendError::Disconnected(returned)) => command = returned,
            }
        }
    }
    Err(command)
}

/// Full routing/command/diagnostic publication through real production types.
/// Fixed-size 4/6-command bursts; no exchange, no fixture construction in send.
#[test]
#[ignore = "offline complete routing boundary and multi-owner burst comparison"]
fn benchmark_owner_burst() {
    use hexagent_runtime::http1_pool::Role;
    use std::io::Write;
    let n:usize=std::env::var("HEX_BURST_N").unwrap_or("10000".into()).parse().unwrap();
    let legacy=std::env::var("HEX_BURST_LEGACY").ok().as_deref()==Some("1");
    const WARM:usize=1000;
    for count in [4usize,6] {
        let mut routes=PolyAccountConnectionRoutes::default();
        let (diagnostic,diagnostics)=hexagent_runtime::poll_channel::bounded(1024);
        let (done,completed)=bounded(count);let mut workers=Vec::new();let mut all_metrics=Vec::new();
        for slot in 0..count {
            let(tx,rx)=hexagent_runtime::poll_channel::bounded_with_wake(1,Some(Arc::new(hexagent_runtime::wake::Wake::default())));
            let mut metrics=PolyConnectionLaneMetrics::new("burst",Role::Cancel,slot);metrics.diagnostic=Some(diagnostic.clone());let metrics=Arc::new(metrics);
            all_metrics.push(metrics.clone());routes.cancel.push(PolyConnectionLane{tx,metrics:metrics.clone(),preparation:None});
            let done=done.clone();workers.push(thread::spawn(move|| {
                for _ in 0..n+WARM {let mut resume=0;
                    let mut command=rx.recv_timeout_observed(Duration::from_secs(5),Duration::from_micros(50),&mut resume).unwrap();
                    let end=crate::types::monotonic_now_ns();let trace=command.hot_path_mut().unwrap();let published=trace.owner_published_mono_ns;
                    metrics.release_for_test();done.send((slot,end,published,resume)).unwrap();
                }
            }));
        }
        routes.cancel_lane_keys.resize(count,None);
        let(updates,_updates_rx)=bounded(1);let update_tx=ExecutorUpdateSender{owner:1,tx:updates.into()};
        let mut rows=Vec::with_capacity(n*count);let mut start=vec![0;count];let mut sends=vec![0;count];let mut received=vec![(0,0,0);count];let mut times=vec![hexagent_runtime::poll_channel::PublicationTiming::default();count];let mut diag_hwm=0;let mut diag_count=0;
        for sequence in 0..n+WARM {
            for slot in 0..count {
                let command=PolyConnectionCommand::Cancel {cancel_trigger:Default::default(),instance_id:"burst".into(),exchange:Exchange::Polymarket,client_order_id:format!("burst-{slot}"),timestamp_ns:1,update_tx:update_tx.clone(),enqueued_at:std::time::Instant::now()};
                start[slot]=crate::types::monotonic_now_ns();let began=std::time::Instant::now();
                assert!(if legacy { legacy_burst_route(&mut routes,Role::Cancel,command) } else { try_send_poly_owner(&mut routes,Role::Cancel,command) }.is_ok());
                sends[slot]=began.elapsed().as_nanos() as u64;
            }
            for _ in 0..count {let(slot,end,published,resume)=completed.recv_timeout(Duration::from_secs(5)).unwrap();received[slot]=(end,published,resume);}
            diag_hwm=diag_hwm.max(diagnostics.len());
            for _ in 0..count {let record=diagnostics.try_recv().unwrap();if let ExecutionDiagnostic::OwnerPublication{slot,publication_mono_ns,timing,..}=record {assert_eq!(publication_mono_ns,received[slot].1);times[slot]=timing;diag_count+=1;}else{panic!("unexpected diagnostic");}}
            if sequence>=WARM {for slot in 0..count {let(end,published,resume)=received[slot];let t=times[slot];rows.push((sends[slot],end-start[slot],end-published,t.push_returned_mono_ns-t.push_started_mono_ns,t.notify_returned_mono_ns-t.push_returned_mono_ns,if resume>0{Some(resume as i128-t.notify_returned_mono_ns as i128)}else{None}));}}
        }
        for w in workers {w.join().unwrap();}
        let prefix=std::env::var("HEX_HTTP_BENCH_OUT").unwrap();let mut f=std::io::BufWriter::new(std::fs::File::create(format!("{prefix}-burst{count}.csv")).unwrap());writeln!(f,"route_send_ns,route_start_to_dequeue_ns,publication_to_dequeue_ns,push_ns,notify_ns,notify_to_resume_ns").unwrap();
        for(a,b,c,d,e,f1)in rows{writeln!(f,"{a},{b},{c},{d},{e},{}",f1.map(|x|x.to_string()).unwrap_or_default()).unwrap();}
        for m in all_metrics {assert_eq!(m.queue_high_water.load(Ordering::Relaxed),1);assert_eq!(m.send_failures.load(Ordering::Relaxed),0);assert_eq!(m.diagnostic_dropped.load(Ordering::Relaxed),0);}
        println!("burst lanes={count} n={} hwm=1 overflow=0 diagnostic_hwm={diag_hwm} diagnostic_records={diag_count} diagnostic_dropped=0",n*count);
    }
}

#[test]
fn account_completion_preserves_fifo_full_retry_disconnect_and_account_isolation() {
    let (a, ar) = account_completion_channel(1);
    let (b, br) = account_completion_channel(1);
    let update = |owner, id: &str| RoutedOrderUpdate {
        owner, update: exec_rejected_cancel(id.into(), Exchange::Polymarket),
        timing: LifecycleTiming::default(),
    };
    a.try_send(update(7, "first")).unwrap();
    let retained = match a.try_send(update(7, "second")) {
        Err(crossbeam_channel::TrySendError::Full(value)) => value,
        _ => panic!("full must retain exact lifecycle"),
    };
    b.try_send(update(9, "first")).unwrap();
    assert_eq!(br.recv().unwrap().owner, 9);
    let first=ar.recv().unwrap();
    assert_eq!((first.owner,first.update.client_order_id.as_str()),(7,"first"));
    a.try_send(retained).unwrap();drop(a);
    let second=ar.recv().unwrap();
    assert_eq!((second.owner,second.update.client_order_id.as_str()),(7,"second"));
    assert!(matches!(ar.try_recv(),Err(crossbeam_channel::TryRecvError::Disconnected)));
    drop(br);
    match b.try_send(update(9,"late")) {
        Err(crossbeam_channel::TrySendError::Disconnected(value)) => {
            assert_eq!(value.owner,9);assert_eq!(value.update.client_order_id,"late");
        }, _ => panic!("disconnect must return ownership"),
    }
}

#[test]
fn account_completion_wakes_consumer_and_drains_before_disconnect() {
    let (tx,rx)=account_completion_channel(2);
    let (ready,started)=std::sync::mpsc::sync_channel(1);
    let (done,finished)=std::sync::mpsc::sync_channel(1);
    let worker=thread::spawn(move || {
        ready.send(()).unwrap();
        let mut ids=Vec::new();
        while let Ok(value)=rx.recv() {ids.push((value.owner,value.update.client_order_id));}
        done.send(ids).unwrap();
    });
    started.recv().unwrap();thread::sleep(Duration::from_millis(10));
    for id in ["one","two"] {tx.send(RoutedOrderUpdate {owner:7,
        update:exec_rejected_cancel(id.into(),Exchange::Polymarket),timing:LifecycleTiming::default()}).unwrap();}
    drop(tx);
    assert_eq!(finished.recv_timeout(Duration::from_secs(2)).unwrap(),vec![(7,"one".into()),(7,"two".into())]);
    worker.join().unwrap();
}
