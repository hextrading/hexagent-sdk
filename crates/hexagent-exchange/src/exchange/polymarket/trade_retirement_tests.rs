use super::*;

fn snapshot_fixture(routes: usize) -> ExecutionStateSnapshot {
    let mut coid_to_oid = HashMap::new();
    let mut oid_to_coid = HashMap::new();
    let mut coid_to_token = HashMap::new();
    for i in 0..routes {
        let coid = format!("owner-{i}");
        let oid = format!("0x{i:064x}");
        coid_to_oid.insert(coid.clone(), oid.clone());
        oid_to_coid.insert(normalize_order_id(&oid), coid.clone());
        coid_to_token.insert(coid, if i < 64 { "retiring" } else { "sibling" }.into());
    }
    ExecutionStateSnapshot {
        coid_to_oid: ExecutionReadMap::from_hash_map(coid_to_oid),
        oid_to_coid: ExecutionReadMap::from_hash_map(oid_to_coid),
        coid_to_token: ExecutionReadMap::from_hash_map(coid_to_token),
        ..ExecutionStateSnapshot::default()
    }
}

#[test]
fn batch_removal_keeps_old_readers_and_unrelated_shards_with_duplicate_keys() {
    let original = snapshot_fixture(256).coid_to_token;
    let first = "owner-0";
    let index = ExecutionReadMap::<String>::shard_index(first);
    let second = original
        .keys()
        .find(|key| key.as_str() != first && ExecutionReadMap::<String>::shard_index(key) == index)
        .unwrap();
    let updated = original.with_removed_keys([first, second.as_str(), first, "absent"]);
    assert_eq!(updated.len(), original.len() - 2);
    assert!(!updated.contains_key(first));
    assert!(!updated.contains_key(second));
    assert!(original.contains_key(first) && original.contains_key(second));
    for shard in 0..EXECUTION_READ_SHARDS {
        if shard != index {
            assert!(Arc::ptr_eq(&original.shards[shard], &updated.shards[shard]));
        }
    }
    let repeated = updated.with_removed_keys([first, second.as_str(), "absent"]);
    assert_eq!(repeated.len(), updated.len());
    assert!(Arc::ptr_eq(&updated.shards, &repeated.shards));
}

#[test]
fn retirement_publishes_coherent_identity_maps_and_preserves_replayed_routes() {
    let original = snapshot_fixture(256);
    let mut owner = ExecutionStateOwner::new(original.clone());
    let owned = HashSet::from(["owner-0".to_string(), "owner-64".to_string()]);
    let retired = reclaim_token_mappings(
        &mut owner.coid_to_oid,
        &mut owner.oid_to_coid,
        &mut owner.coid_to_token,
        &["retiring".into()],
        Some(&owned),
    );
    assert_eq!(
        retired.len(),
        1,
        "the sibling token is outside this retirement scope"
    );
    let mut next = original.clone();
    retired.apply_to(&mut next);
    assert!(!next.coid_to_oid.contains_key("owner-0"));
    assert!(!next.coid_to_token.contains_key("owner-0"));
    assert!(!next.oid_to_coid.contains_key(&format!("{:064x}", 0)));
    assert_eq!(
        next.coid_to_token.get("owner-64").map(String::as_str),
        Some("sibling")
    );
    assert!(
        original.coid_to_oid.contains_key("owner-0"),
        "old reader stays valid"
    );
    // Repeated eviction after a later replay in another scope must not erase it.
    owner
        .coid_to_oid
        .insert("owner-0".into(), "0xabcdef".into());
    owner.oid_to_coid.insert("abcdef".into(), "owner-0".into());
    owner
        .coid_to_token
        .insert("owner-0".into(), "replayed-event".into());
    let repeated = reclaim_token_mappings(
        &mut owner.coid_to_oid,
        &mut owner.oid_to_coid,
        &mut owner.coid_to_token,
        &["retiring".into()],
        Some(&owned),
    );
    assert_eq!(repeated.len(), 0);
    assert_eq!(
        owner.oid_to_coid.get("abcdef").map(String::as_str),
        Some("owner-0")
    );
    let prior_shards = Arc::clone(&next.coid_to_oid.shards);
    repeated.apply_to(&mut next);
    assert!(Arc::ptr_eq(&prior_shards, &next.coid_to_oid.shards));
}

#[test]
fn duplicate_retirement_command_does_not_republish_an_unchanged_snapshot() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    shutdown.request();
    shutdown.finish();
    assert_eq!(shared.join_background_workers(), 3);
    // The test thread takes ownership after the live owner has joined.
    let initial = snapshot_fixture(256);
    let mut owner = ExecutionStateOwner::new(initial.clone());
    shared.execution_state.store(Arc::new(initial));
    let command = || ExecutionStateCommand::RetireMappings {
        asset_ids: vec!["retiring".into()],
        owned_coids: HashSet::from(["owner-0".into()]),
    };
    owner.apply(&shared, command());
    let retired = shared.execution_state.load_full();
    assert!(!retired.coid_to_oid.contains_key("owner-0"));
    assert!(retired.coid_to_oid.contains_key("owner-64"));
    owner.apply(&shared, command());
    assert!(Arc::ptr_eq(&retired, &shared.execution_state.load_full()));
}

#[test]
#[ignore = "focused old/new retirement publication and following-message queue benchmark"]
fn benchmark_retirement_snapshot_publication() {
    const N: usize = 1000;
    const ROUTES: usize = 8192;
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    shutdown.request();
    shutdown.finish();
    assert_eq!(shared.join_background_workers(), 3);
    for targets in [0, 64] {
        let original = Arc::new(snapshot_fixture(ROUTES));
        let mut owner = ExecutionStateOwner::new((*original).clone());
        let owned: HashSet<String> = (0..targets).map(|i| format!("owner-{i}")).collect();
        let retired = reclaim_token_mappings(
            &mut owner.coid_to_oid,
            &mut owner.oid_to_coid,
            &mut owner.coid_to_token,
            &["retiring".into()],
            Some(&owned),
        );
        assert_eq!(retired.len(), targets);
        for baseline in [true, false] {
            let mut samples = Vec::with_capacity(N);
            let (tx, rx) = crossbeam_channel::bounded(2);
            for _ in 0..N {
                // Retain an old reader and restore outside the measured window.
                shared.execution_state.store(Arc::clone(&original));
                tx.try_send(None).unwrap();
                tx.try_send(Some(std::time::Instant::now())).unwrap();
                assert_eq!(rx.len(), 2);
                assert!(rx.recv().unwrap().is_none());
                if baseline {
                    owner.publish(&shared, false, true);
                } else if retired.len() != 0 {
                    let mut next = (*shared.execution_state.load_full()).clone();
                    retired.apply_to(&mut next);
                    shared.execution_state.store(Arc::new(next));
                }
                samples.push(rx.recv().unwrap().unwrap().elapsed().as_nanos() as u64);
            }
            let view = shared.execution_state.load_full();
            assert_eq!(view.coid_to_oid.len(), ROUTES - targets);
            assert_eq!(view.oid_to_coid.len(), ROUTES - targets);
            assert_eq!(view.coid_to_token.len(), ROUTES - targets);
            assert!(original.coid_to_oid.contains_key("owner-0"));
            samples.sort_unstable();
            let q = |v: usize| samples[(N * v).div_ceil(1000) - 1];
            eprintln!("retirement_publication_probe mode={} n={N} routes={ROUTES} targets={targets} boundary=private_enqueue_to_dequeue_after_publication p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_peak=2 capacity=2 overflow=0",
                if baseline {"full_rebuild"} else {"changed_shards"},q(500),q(990),q(999),q(1000));
        }
    }
}
