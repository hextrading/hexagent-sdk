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
    assert_eq!(trace.identity_probes, 1);
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

fn colliding_ids(index: &RuntimeOwnershipIndex, count: usize) -> Vec<String> {
    (0_u64..).map(|n| format!("0x{n:064x}"))
        .filter(|id| index.start_index(id) == 0).take(count).collect()
}

#[test]
fn ownership_collision_publish_remove_replay_and_full_budget_preserve_routes() {
    let index = RuntimeOwnershipIndex::with_capacity(256);
    let ids = colliding_ids(&index, RUNTIME_OWNERSHIP_MAX_PROBES + 1);
    for (n, id) in ids.iter().take(RUNTIME_OWNERSHIP_MAX_PROBES).enumerate() {
        let mut route = runtime_ownership(id, &format!("coid-{n}"));
        route.instance_id = format!("owner-{}", n % 3);
        let (_, probes) = index.publish_counted(id, route).unwrap();
        assert_eq!(usize::from(probes), n + 1);
    }
    let retained = index.find_entry(&ids[60]).unwrap();
    let last = ids.last().unwrap();
    assert!(index.insert(last, runtime_ownership(last, "overflow")).is_err());
    assert!(!index.contains(last));
    index.remove(&ids[20]);
    index.insert(last, runtime_ownership(last, "after-hole")).unwrap();
    assert!(Arc::ptr_eq(&retained, &index.find_entry(&ids[60]).unwrap()));
    assert_eq!(retained.ownership.instance_id, "owner-0");
    assert_eq!(index.client_order_id(last).as_deref(), Some("after-hole"));
    index.remove(last);
    index.insert(&ids[20], runtime_ownership(&ids[20], "replayed")).unwrap();
    assert_eq!(index.client_order_id(&ids[20]).as_deref(), Some("replayed"));
    for (n, id) in ids.iter().take(RUNTIME_OWNERSHIP_MAX_PROBES).enumerate() {
        if n != 20 { assert_eq!(index.client_order_id(id), Some(format!("coid-{n}"))); }
    }
}

#[test]
fn concurrent_collision_writers_never_replace_a_sibling_oid() {
    let index = Arc::new(RuntimeOwnershipIndex::with_capacity(256));
    let ids = Arc::new(colliding_ids(&index, 32));
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let workers: Vec<_> = (0..4).map(|owner| {
        let (index, ids, barrier) = (index.clone(), ids.clone(), barrier.clone());
        std::thread::spawn(move || {
            barrier.wait();
            for round in 0..100 {
                for n in (owner..32).step_by(4) {
                    let id = &ids[n];
                    let coid = format!("owner-{owner}-{n}-{round}");
                    index.insert(id, runtime_ownership(id, &coid)).unwrap();
                    assert_eq!(index.client_order_id(id).as_deref(), Some(coid.as_str()));
                    if round != 99 { index.remove(id); }
                }
            }
        })
    }).collect();
    for worker in workers { worker.join().unwrap(); }
    for n in 0..32 {
        assert_eq!(index.client_order_id(&ids[n]), Some(format!("owner-{}-{n}-99", n % 4)));
    }
}

// The retired implementation is kept only in this ignored measurement. It
// performs a publication even for every unrelated occupied probe.
fn legacy_publish(index: &RuntimeOwnershipIndex, id: &str, ownership: OrderOwnership) {
    let entry = Arc::new(RuntimeOwnershipEntry { normalized_order_id: RuntimeOrderId::new(id), ownership });
    let normalized = entry.normalized_order_id.as_ref();
    let start = index.start_index(normalized);
    for offset in 0..RUNTIME_OWNERSHIP_MAX_PROBES {
        let slot = &index.slots[(start + offset) % index.slots.len()];
        slot.rcu(|current| match current {
            Some(old) if old.normalized_order_id.as_ref() != normalized => Some(Arc::clone(old)),
            _ => Some(Arc::clone(&entry)),
        });
        if slot.load().as_ref().is_some_and(|old| old.normalized_order_id.as_ref() == normalized) { return; }
    }
    panic!("benchmark probe budget exhausted");
}

#[test]
#[ignore = "release publication benchmark with fixed collision depth and registered readers"]
fn ownership_collision_publication_benchmark() {
    use std::{hint::black_box, time::Instant};
    let index = Arc::new(RuntimeOwnershipIndex::with_capacity(256));
    let ids = colliding_ids(&index, 65);
    let ready = Arc::new(std::sync::Barrier::new(33));
    let done = Arc::new(std::sync::Barrier::new(33));
    let readers: Vec<_> = (0..32).map(|_| {
        let (index, ready, done) = (index.clone(), ready.clone(), done.clone());
        std::thread::spawn(move || {
            let held = index.slots[255].load();
            ready.wait(); done.wait(); drop(held);
        })
    }).collect();
    ready.wait();
    for depth in [0, 8, 32, 64] {
        for id in &ids[..depth] { index.insert(id, runtime_ownership(id, "sibling")).unwrap(); }
        let id = &ids[depth];
        for legacy in [true, false] {
            let mut samples = Vec::with_capacity(10_000);
            for i in 0..11_000 {
                let ownership = runtime_ownership(id, "target");
                let start = Instant::now();
                if legacy { legacy_publish(&index, black_box(id), ownership); }
                else { index.insert(black_box(id), ownership).unwrap(); }
                let ns = start.elapsed().as_nanos() as u64;
                if i >= 1000 { samples.push(ns); }
                index.remove(id);
            }
            samples.sort_unstable(); let n = samples.len();
            eprintln!("ownership_collision mode={} collisions={depth} registered_readers=32 n={n} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=prepared_ownership_to_identity_publication excludes=input_build_and_removal",
                if legacy { "before" } else { "after" }, (samples[n/2-1]+samples[n/2])/2,
                samples[(n*99).div_ceil(100)-1], samples[(n*999).div_ceil(1000)-1], samples[n-1]);
        }
    }
    done.wait(); for reader in readers { reader.join().unwrap(); }
}
