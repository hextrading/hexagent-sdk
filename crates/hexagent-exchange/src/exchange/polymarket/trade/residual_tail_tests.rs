use super::tests::runtime_ownership;
use super::*;

#[test]
fn prepared_registration_publishes_before_handoff_and_rolls_back_full_or_disconnected() {
    let index = RuntimeOwnershipIndex::new();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let mut trace = crate::types::HotPathTrace::default();
    enqueue_prepared_order(
        &index,
        &tx,
        "0xABC",
        runtime_ownership("0xABC", "first"),
        &mut trace,
    )
    .unwrap();
    assert_eq!(index.get("abc").unwrap().client_order_id, "first");
    assert!(trace.identity_published_mono_ns > 0);
    assert!(trace.registration_enqueued_mono_ns >= trace.identity_published_mono_ns);
    let mut failed = crate::types::HotPathTrace::default();
    assert!(enqueue_prepared_order(
        &index,
        &tx,
        "0xDEF",
        runtime_ownership("0xDEF", "second"),
        &mut failed
    )
    .is_err());
    assert!(!index.contains("def"));
    assert_eq!(failed.registration_enqueued_mono_ns, 0);
    let AccountLifecycleJob::RegisterPreparedOrder(entry) = rx.try_recv().unwrap() else {
        panic!("one combined message")
    };
    assert!(Arc::ptr_eq(&entry, &index.find_entry("ABC").unwrap()));
    assert_eq!(entry.ownership.instance_id, "maker");
    assert!(rx.is_empty());
    drop(rx);
    assert!(enqueue_prepared_order(
        &index,
        &tx,
        "0x123",
        runtime_ownership("0x123", "third"),
        &mut failed
    )
    .is_err());
    assert!(!index.contains("123"));
    assert_eq!(index.get("ABC").unwrap().client_order_id, "first");
}

#[test]
fn inline_and_legacy_oid_normalization_preserve_rebound_and_instance_identity() {
    let index = RuntimeOwnershipIndex::new();
    let mut first = runtime_ownership("0xABC", "same-coid");
    first.instance_id = "owner-a".into();
    index.insert(" 0XAbC ", first).unwrap();
    assert_eq!(index.get("abc").unwrap().instance_id, "owner-a");
    assert!(matches!(
        index.find_entry("abc").unwrap().normalized_order_id,
        RuntimeOrderId::Inline(_)
    ));
    let long = "A".repeat(256);
    let mut second = runtime_ownership(&long, "same-coid");
    second.instance_id = "owner-b".into();
    index.insert(&long, second).unwrap();
    assert_eq!(
        index.get(&long.to_ascii_lowercase()).unwrap().instance_id,
        "owner-b"
    );
    index.remove("abc");
    assert!(index.get("ABC").is_none());
    assert!(index.get(&long).is_some());
}

#[test]
fn combined_registration_commits_identity_and_open_order_in_one_snapshot() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    let mut execution = ExecutionStateOwner::new(ExecutionStateSnapshot::default());
    execution.apply(
        &shared,
        ExecutionStateCommand::InstallIdentity {
            client_order_id: "coid-a".into(),
            exchange_order_id: "0xABC".into(),
            token: "UP".into(),
            tracked: Some(TrackedOrder {
                order_slot: OrderSlot::with_generation(3, 7),
                symbol: "UP".into(),
                side: Side::Buy,
                instance_id: "owner-a".into(),
            }),
        },
    );
    let published = shared.execution_snapshot();
    assert_eq!(
        published.coid_to_oid.get("coid-a").map(String::as_str),
        Some("0xABC")
    );
    assert_eq!(
        published.oid_to_coid.get("abc").map(String::as_str),
        Some("coid-a")
    );
    assert_eq!(published.open_orders["coid-a"].instance_id, "owner-a");
    assert_eq!(
        published.open_orders["coid-a"].order_slot,
        OrderSlot::with_generation(3, 7)
    );
    assert!(!published.open_orders.contains_key("coid-b"));
    shutdown.request();
    shutdown.finish();
    shared.join_background_workers();
}

fn report(label: &str, mut samples: Vec<u64>, high_water: usize) {
    samples.sort_unstable();
    let n = samples.len();
    let rank =
        |numerator: usize, denominator: usize| samples[(n * numerator).div_ceil(denominator) - 1];
    eprintln!("residual_bench={label} n={n} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water={high_water} overflow=0 boundary=prepared_ownership_to_identity_and_registration_enqueue",
        (samples[(n-1)/2] + samples[n/2])/2, rank(99,100), rank(999,1000), samples[n-1]);
}

#[test]
#[ignore = "focused release benchmark; no network or account persistence"]
fn residual_registration_burst_benchmark() {
    // The old two-message path uses the same optimized index to isolate the
    // eliminated clones/queue handoff. Thus this does not claim total before/after
    // speedup including the separate inline OID/hash improvement.
    for burst in [1, 2, 3, 6] {
        for combined in [false, true] {
            let index = RuntimeOwnershipIndex::new();
            let (tx, rx) = crossbeam_channel::bounded(64);
            let mut samples = Vec::with_capacity(6000 * burst);
            let mut high_water = 0;
            let ids: Vec<_> = (0..burst).map(|i| format!("0x{i:064x}")).collect();
            for iteration in 0..6100 {
                for (i, oid) in ids.iter().enumerate() {
                    let coid = format!("owner-{iteration}-{i}");
                    let ownership = runtime_ownership(oid, &coid);
                    let start = std::time::Instant::now();
                    if combined {
                        enqueue_prepared_order(
                            &index,
                            &tx,
                            oid,
                            ownership,
                            &mut crate::types::HotPathTrace::default(),
                        )
                        .unwrap();
                    } else {
                        index.insert(oid, ownership.clone()).unwrap();
                        tx.try_send(AccountLifecycleJob::RegisterLocalOrder {
                            command: ExecutionStateCommand::InstallIdentity {
                                client_order_id: coid.clone(),
                                exchange_order_id: oid.clone(),
                                token: "UP".into(),
                                tracked: None,
                            },
                            ownership: Some(ownership),
                        })
                        .unwrap();
                        tx.try_send(AccountLifecycleJob::ExecutionState(
                            ExecutionStateCommand::TrackOpen {
                                client_order_id: coid.clone(),
                                tracked: TrackedOrder {
                                    order_slot: Default::default(),
                                    symbol: "UP".into(),
                                    side: Side::Buy,
                                    instance_id: "maker".into(),
                                },
                            },
                        ))
                        .unwrap();
                    }
                    let ns = start.elapsed().as_nanos() as u64;
                    if iteration >= 100 {
                        samples.push(ns);
                    }
                }
                high_water = high_water.max(tx.len());
                while rx.try_recv().is_ok() {}
                for oid in &ids {
                    index.remove(oid);
                }
            }
            report(
                &format!(
                    "{}_burst{burst}",
                    if combined { "combined" } else { "two_messages" }
                ),
                samples,
                high_water,
            );
        }
    }
}
