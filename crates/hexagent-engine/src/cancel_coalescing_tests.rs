use super::*;
use hexagent_runtime::http1_pool::Role;

fn fixture(
    slots: usize,
) -> (
    PolyAccountConnectionRoutes,
    Vec<Receiver<PolyConnectionCommand>>,
    ExecutorUpdateSender,
) {
    let mut routes = PolyAccountConnectionRoutes::default();
    let mut receivers = vec![];
    for slot in 0..slots {
        let (tx, rx) = bounded(1);
        routes
            .cancel
            .push(PolyConnectionLane::for_test(tx, Role::Cancel, slot));
        routes.cancel_lane_keys.push(None);
        receivers.push(rx);
    }
    let (tx, _) = bounded(8);
    (routes, receivers, ExecutorUpdateSender { owner: 1, tx: tx.into() })
}

fn command(iid: &str, coid: &str, sender: &ExecutorUpdateSender) -> PolyConnectionCommand {
    PolyConnectionCommand::Cancel {
        instance_id: iid.into(),
        exchange: Exchange::Polymarket,
        client_order_id: coid.into(),
        timestamp_ns: 1,
        update_tx: sender.clone(),
        enqueued_at: Instant::now(),
    }
}

#[test]
fn duplicate_cancel_uses_one_connection_and_keeps_sibling_capacity() {
    let (mut routes, receivers, sender) = fixture(3);
    for _ in 0..100 {
        assert!(
            send_poly_owner_lossless(&mut routes, Role::Cancel, command("a", "same", &sender))
                .is_ok()
        );
    }
    assert_eq!(routes.cancel_outbox_stats.coalesced, 99);
    assert_eq!(receivers.iter().map(Receiver::len).sum::<usize>(), 1);
    assert!(routes.cancel_outbox.is_empty());
    assert!(
        send_poly_owner_lossless(&mut routes, Role::Cancel, command("a", "other", &sender)).is_ok()
    );
    assert_eq!(receivers.iter().map(Receiver::len).sum::<usize>(), 2);
    assert!(routes.safety_cancel_outbox.is_empty());
}

#[test]
fn queued_duplicate_is_coalesced_and_fifo_retry_survives_completion_publication() {
    let (mut routes, receivers, sender) = fixture(1);
    for coid in ["first", "second", "second", "third"] {
        assert!(
            send_poly_owner_lossless(&mut routes, Role::Cancel, command("a", coid, &sender))
                .is_ok()
        );
    }
    assert_eq!(routes.cancel_outbox.len(), 2);
    receivers[0].recv().unwrap();
    // A retry caused by the published result must survive until occupancy is released.
    routes.cancel[0]
        .metrics
        .cancel_reply_pending
        .store(false, Ordering::Release);
    assert!(
        send_poly_owner_lossless(&mut routes, Role::Cancel, command("a", "first", &sender)).is_ok()
    );
    assert_eq!(routes.cancel_outbox.len(), 3);
    for expected in ["second", "third", "first"] {
        routes.cancel[0].metrics.release_for_test();
        flush_poly_cancel_outbox(&mut routes);
        assert!(
            matches!(receivers[0].recv().unwrap(), PolyConnectionCommand::Cancel { client_order_id, .. } if client_order_id == expected)
        );
    }
    assert!(routes.cancel_outbox_keys.is_empty());
}

#[test]
fn coalescing_preserves_instance_identity_and_full_queue_feedback() {
    let (mut routes, _receivers, sender) = fixture(1);
    assert!(
        send_poly_owner_lossless(&mut routes, Role::Cancel, command("a", "same", &sender)).is_ok()
    );
    assert!(
        send_poly_owner_lossless(&mut routes, Role::Cancel, command("b", "same", &sender)).is_ok()
    );
    for i in 1..POLY_CANCEL_OUTBOX_CAPACITY {
        assert!(send_poly_owner_lossless(
            &mut routes,
            Role::Cancel,
            command("a", &format!("q{i}"), &sender)
        )
        .is_ok());
    }
    assert!(
        send_poly_owner_lossless(&mut routes, Role::Cancel, command("b", "same", &sender)).is_ok()
    );
    assert!(
        send_poly_owner_lossless(&mut routes, Role::Cancel, command("a", "overflow", &sender))
            .is_err()
    );
    assert_eq!(routes.cancel_outbox_stats.overflow, 1);
    assert_eq!(routes.cancel_outbox.len(), POLY_CANCEL_OUTBOX_CAPACITY);
    assert_eq!(routes.cancel_outbox_keys.len(), POLY_CANCEL_OUTBOX_CAPACITY);
    let mut other_sender = sender.clone();
    other_sender.owner = 2;
    assert_ne!(
        command("a", "same", &sender).cancel_key(),
        command("a", "same", &other_sender).cancel_key()
    );
    assert!(command("a", &"x".repeat(129), &sender)
        .cancel_key()
        .is_none());
}

#[test]
#[ignore = "paired repeat-cancel admission profile; run release serially"]
fn repeated_cancel_admission_latency_profile() {
    const N: usize = 10_000;
    let mut fixtures = [fixture(3), fixture(3)];
    let mut samples = [Vec::with_capacity(N * 4), Vec::with_capacity(N * 4)];
    let mut peak = [0; 2];
    for iteration in 0..N + 64 {
        for version in [iteration % 2, 1 - iteration % 2] {
            let (routes, receivers, sender) = &mut fixtures[version];
            for coid in ["A", "B", "A", "A"] {
                let request = command("btc01", coid, sender);
                let start = Instant::now();
                if version == 0 {
                    // Previous dispatcher: every repeated intent consumes
                    // another owner or joins its FIFO, even for the same OID.
                    flush_poly_cancel_outbox(routes);
                    if let Err(command) = try_send_poly_owner(routes, Role::Cancel, request) {
                        routes.cancel_outbox.push_back(command);
                    }
                } else {
                    assert!(send_poly_owner_lossless(routes, Role::Cancel, request).is_ok());
                }
                let elapsed = start.elapsed().as_nanos() as u64;
                if iteration >= 64 {
                    samples[version].push(elapsed);
                }
                peak[version] = peak[version].max(routes.cancel_outbox.len());
            }
            assert_eq!(
                receivers.iter().map(Receiver::len).sum::<usize>(),
                if version == 0 { 3 } else { 2 }
            );
            for rx in receivers {
                while rx.try_recv().is_ok() {}
            }
            for lane in &routes.cancel {
                lane.metrics.release_for_test();
            }
            routes.cancel_outbox.clear();
            routes.cancel_outbox_keys.clear();
        }
    }
    for (version, s) in samples.iter_mut().enumerate() {
        s.sort_unstable();
        let n = s.len();
        eprintln!("repeat_cancel version={version} n={n} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water={} overflow=0 boundary=dispatcher_admission excludes=command_construction_http_response_scheduling", s[n/2], s[n*99/100], s[n*999/1000], s[n-1], peak[version]);
    }
}
