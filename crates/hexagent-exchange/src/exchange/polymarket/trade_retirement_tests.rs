use super::*;
use crate::account::shared_account::SharedAccount;

#[test]
fn shared_execution_rows_preserve_old_readers_without_cloning_history_values() {
    #[derive(Debug)]
    struct Value(usize, Arc<std::sync::atomic::AtomicUsize>);
    impl Clone for Value {
        fn clone(&self) -> Self {
            self.1.fetch_add(1, Ordering::Relaxed);
            Self(self.0, Arc::clone(&self.1))
        }
    }
    let clones = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let old = ExecutionReadMap::from_hash_map((0..20_000)
        .map(|i| (format!("key-{i}"), Value(i, Arc::clone(&clones)))).collect());
    let changed = old.with_insert("key-0".into(), Value(99, Arc::clone(&clones)));
    let sibling = old.with_insert("key-0".into(), Value(88, Arc::clone(&clones)));
    assert_eq!(old.get("key-0").unwrap().0, 0);
    assert_eq!(changed.get("key-0").unwrap().0, 99);
    assert_eq!(sibling.get("key-0").unwrap().0, 88);
    assert_eq!(changed.len(), old.len());
    let removed = changed.with_removed_keys(["key-0", "key-0", "absent"]);
    assert!(!removed.contains_key("key-0"));
    assert_eq!(removed.len(), old.len() - 1);
    assert_eq!(removed.get("key-1").unwrap().0, 1);
    // Reconnect/replay can publish the identity again without changing readers
    // that retained either the removed or the pre-rebind generation.
    let replayed = removed.with_insert("key-0".into(), Value(77, Arc::clone(&clones)));
    assert_eq!(replayed.len(), old.len());
    assert_eq!(replayed.get("key-0").unwrap().0, 77);
    assert_eq!(changed.get("key-0").unwrap().0, 99);
    assert_eq!(clones.load(Ordering::Relaxed), 0);
}

#[test]
#[ignore = "release: deployed owned-string leaves vs shared rows, publication plus destruction"]
fn benchmark_shared_execution_rows() {
    type OldShard = [Arc<HashMap<String, String>>; EXECUTION_READ_LEAVES];
    #[derive(Clone)]
    struct OldMap(Arc<[Arc<OldShard>; EXECUTION_READ_SHARDS]>);
    impl OldMap {
        fn from_map(values: HashMap<String, String>) -> Self {
            let mut shards: [[HashMap<String, String>; EXECUTION_READ_LEAVES]; EXECUTION_READ_SHARDS] =
                std::array::from_fn(|_| std::array::from_fn(|_| HashMap::new()));
            for (k, v) in values {
                shards[ExecutionReadMap::<String>::shard_index(&k)]
                    [ExecutionReadMap::<String>::leaf_index(&k)].insert(k, v);
            }
            Self(Arc::new(shards.map(|s| Arc::new(s.map(Arc::new)))))
        }
        fn insert(&self, k: String, v: String) -> Self {
            let i = ExecutionReadMap::<String>::shard_index(&k);
            let j = ExecutionReadMap::<String>::leaf_index(&k);
            let mut outer = (*self.0).clone();
            let mut inner = (*outer[i]).clone();
            Arc::make_mut(&mut inner[j]).insert(k, v);
            outer[i] = Arc::new(inner);
            Self(Arc::new(outer))
        }
        fn get(&self, k: &str) -> &String {
            let h = ExecutionReadMap::<String>::hash(k);
            &self.0[h % EXECUTION_READ_SHARDS][h / EXECUTION_READ_SHARDS % EXECUTION_READ_LEAVES][k]
        }
    }
    const HISTORY: usize = 45_000;
    const N: usize = 5_000;
    let maps: [HashMap<String, String>; 3] = std::array::from_fn(|kind| (0..HISTORY).map(|i| {
        let coid = format!("btc01-{i:013}"); let oid = format!("{i:064x}");
        match kind { 0 => (coid, oid), 1 => (oid, coid), _ => (coid, format!("{i:077}")) }
    }).collect());
    let mut old = maps.clone().map(OldMap::from_map);
    let mut new = maps.map(ExecutionReadMap::from_hash_map);
    let mut old_ns = Vec::with_capacity(N); let mut new_ns = Vec::with_capacity(N);
    let mut old_read = Vec::with_capacity(N); let mut new_read = Vec::with_capacity(N);
    for i in HISTORY..HISTORY + N {
        let coid = format!("btc01-{i:013}"); let oid = format!("{i:064x}"); let token = format!("{i:077}");
        // Clone command inputs before the timers, identically for both paths.
        let mut before = Some([(coid.clone(), oid.clone()), (oid.clone(), coid.clone()), (coid.clone(), token.clone())]);
        let mut after = before.clone();
        for baseline in if i % 2 == 0 { [true, false] } else { [false, true] } {
            if baseline {
                let values = before.take().unwrap();
                let start = std::time::Instant::now();
                for (map, (k, v)) in old.iter_mut().zip(values) { *map = map.insert(k, v); }
                old_ns.push(start.elapsed().as_nanos() as u64);
                let start = std::time::Instant::now();
                std::hint::black_box(old[0].get(&coid));
                old_read.push(start.elapsed().as_nanos() as u64);
            } else {
                let values = after.take().unwrap();
                let start = std::time::Instant::now();
                for (map, (k, v)) in new.iter_mut().zip(values) { *map = map.with_insert(k, v); }
                new_ns.push(start.elapsed().as_nanos() as u64);
                let start = std::time::Instant::now();
                std::hint::black_box(new[0].get(&coid));
                new_read.push(start.elapsed().as_nanos() as u64);
            }
        }
        assert_eq!(new[0].get(&coid), Some(old[0].get(&coid)));
        assert_eq!(new[1].get(&oid), Some(old[1].get(&oid)));
        assert_eq!(new[2].get(&coid), Some(old[2].get(&coid)));
    }
    for (boundary, mut values) in [("owned_rows_publish_drop", old_ns), ("shared_rows_publish_drop", new_ns),
        ("owned_rows_lookup", old_read), ("shared_rows_lookup", new_read)] {
        values.sort_unstable(); let q = |p: usize| values[(N*p).div_ceil(1000)-1];
        eprintln!("execution_rows boundary={boundary} n={N} history={HISTORY} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0", q(500), q(990), q(999), q(1000));
    }
}

#[test]
#[ignore = "release: live-sized identity publication, subsequent reader, no network"]
fn benchmark_live_identity_publication() {
    const HISTORY: usize = 26_500;
    const N: usize = 2_000;
    let mut view = snapshot_fixture(HISTORY);
    let changes: Vec<_> = (HISTORY..HISTORY + N + 64).map(|i|
        (format!("btc01-{i:013}"), format!("0x{i:064x}"), format!("{i:077}"))).collect();
    let mut times = Vec::with_capacity(N);
    let mut following = Vec::with_capacity(N);
    for (i, (coid, oid, token)) in changes.iter().enumerate() {
        let start = std::time::Instant::now();
        let previous = view;
        let mut next = previous.clone();
        next.coid_to_oid = next.coid_to_oid.with_insert(coid.clone(), oid.clone());
        next.oid_to_coid = next.oid_to_coid.with_insert(normalize_order_id(oid), coid.clone());
        next.coid_to_token = next.coid_to_token.with_insert(coid.clone(), token.clone());
        view = next;
        drop(previous); // include destruction; do not hide it in setup.
        let published = start.elapsed().as_nanos() as u64;
        assert_eq!(view.coid_to_oid.get(coid), Some(oid));
        if i >= 64 {
            times.push(published);
            following.push(start.elapsed().as_nanos() as u64);
        }
    }
    for (boundary, values) in [("identity_publication_and_drop", &mut times), ("following_reader", &mut following)] {
        values.sort_unstable();
        let q = |p: usize| values[(N*p).div_ceil(1000)-1];
        eprintln!("live_identity_probe boundary={boundary} N={N} history={HISTORY} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0", q(500), q(990), q(999), q(1000));
    }
}

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
fn retirement_requires_terminal_owner_certificate_and_duplicate_does_not_republish() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let mut shared = trade.shared_state();
    shutdown.request();
    shutdown.finish();
    assert_eq!(shared.join_background_workers(), 3);
    drop(trade);
    // No production worker remains. Give this deterministic fixture an unbound
    // account and make the test thread the sole execution/GC owner.
    let shared = Arc::get_mut(&mut shared).expect("joined workers release shared state");
    let account = Arc::new(SharedAccount::new("retirement-certificate"));
    account.register_instance("owner", 1.0);
    account
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    account
        .reserve_order(
            "owner",
            "owner-0",
            &format!("0x{:064x}", 0),
            "retiring",
            Side::Buy,
            1.0,
            0.4,
            0,
        )
        .unwrap();
    account.mark_order_status("owner-0", OrderStatus::Accepted);
    let tokens = vec!["retiring".into()];
    account
        .retain_settled_event_audit("owner", "condition", &tokens)
        .unwrap();
    account
        .release_settled_event_audit("owner", "condition", &tokens)
        .unwrap();
    let mut cold_owner = account.register_settled_gc_cold_owner("owner").unwrap();
    shared.account_state = Arc::clone(&account);
    let initial = snapshot_fixture(256);
    let mut execution = ExecutionStateOwner::new(initial.clone());
    let mut live_position = LivePositionManager::new();
    shared.execution_state.store(Arc::new(initial));
    let before = shared.execution_state.load_full();

    assert_eq!(
        shared.finalize_ready_settled_audit_retirements(&mut execution, &mut live_position),
        (0, 0)
    );
    assert!(
        Arc::ptr_eq(&before, &shared.execution_state.load_full()),
        "no certificate cannot retire routes"
    );
    cold_owner.poll_once().unwrap();
    assert_eq!(
        shared.finalize_ready_settled_audit_retirements(&mut execution, &mut live_position),
        (0, 0)
    );
    assert!(
        Arc::ptr_eq(&before, &shared.execution_state.load_full()),
        "Accepted reserve must preserve query/replay identity"
    );
    assert_eq!(account.order("owner-0").unwrap().reserved_cash, 0.4);

    account
        .apply_authoritative_order_audit(
            "owner-0",
            OrderStatus::Cancelled,
            &AuthoritativeOrderAudit {
                original_size: Some("1".into()),
                size_matched: Some("0".into()),
                associate_trades: vec![],
            },
        )
        .unwrap();
    account.note_settled_gc_activity();
    assert_eq!(
        shared.finalize_ready_settled_audit_retirements(&mut execution, &mut live_position),
        (0, 0)
    );
    assert!(
        shared
            .execution_snapshot()
            .coid_to_oid
            .contains_key("owner-0")
    );
    cold_owner.poll_once().unwrap();
    assert_eq!(
        shared
            .finalize_ready_settled_audit_retirements(&mut execution, &mut live_position)
            .0,
        1
    );
    let retired = shared.execution_state.load_full();
    assert!(!retired.coid_to_oid.contains_key("owner-0"));
    assert!(retired.coid_to_oid.contains_key("owner-64"));
    assert!(
        before.coid_to_oid.contains_key("owner-0"),
        "previous immutable reader remains valid"
    );
    assert_eq!(
        shared.finalize_ready_settled_audit_retirements(&mut execution, &mut live_position),
        (0, 0)
    );
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
            eprintln!(
                "retirement_publication_probe mode={} n={N} routes={ROUTES} targets={targets} boundary=private_enqueue_to_dequeue_after_publication p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_peak=2 capacity=2 overflow=0",
                if baseline {
                    "full_rebuild"
                } else {
                    "changed_shards"
                },
                q(500),
                q(990),
                q(999),
                q(1000)
            );
        }
    }
}
