use super::*;

#[test]
fn unused_fee_registry_is_typed_ordered_idempotent_and_durable() {
    let _guard = super::tests::persistence_test_guard();
    let path = std::env::temp_dir().join(format!(
        "fee-tail-{}-{}.json",
        std::process::id(),
        wall_clock_ms()
    ));
    let account = Arc::new(SharedAccount::new_persistent("fees", &path).unwrap());
    account.register_instance("a", 1.0);
    account.register_instance("b", 1.0);
    account
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    let (_, cold) = account.bind_account_owner().unwrap();
    cold.mark_current_thread().unwrap();
    let lifecycle = account.bind_account_lifecycle_owner().unwrap();
    lifecycle.mark_current_thread().unwrap();
    let locks = account.account_lock_acquisitions.load(Ordering::Acquire);
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.02,
            1.0,
            FeeSettlement::LegacyV1,
        )
        .unwrap();
    let generation = account
        .persistence
        .as_ref()
        .unwrap()
        .next_generation
        .load(Ordering::Acquire);
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.02,
            1.0,
            FeeSettlement::LegacyV1,
        )
        .unwrap();
    assert_eq!(
        account
            .persistence
            .as_ref()
            .unwrap()
            .next_generation
            .load(Ordering::Acquire),
        generation
    );
    assert_eq!(
        account.account_lock_acquisitions.load(Ordering::Acquire),
        locks,
        "new fee registration must never enter the aggregate transaction"
    );
    // An unrelated cold publication must not roll back a pending registry delta.
    {
        let state = account.state.lock().unwrap();
        account.publish_control_snapshots(&state);
    }
    assert_eq!(account.token_fee_configs_fast.load()["UP"].rate, 0.02);
    account
        .reserve_order("a", "coid", "oid", "UP", Side::Buy, 5.0, 0.4, 0)
        .unwrap();
    account
        .apply_trade_transition("trade", "MATCHED", "coid", "oid", "UP", Side::Buy, 5.0, 0.4)
        .unwrap();
    // A revision with an existing execution uses the provenance-preserving
    // fallback. Its later mirror must win over the still-queued initial config.
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.03,
            1.0,
            FeeSettlement::LegacyV1,
        )
        .unwrap();
    cold.execute_lifecycle_mirror();
    assert_eq!(
        account.state.lock().unwrap().token_fee_configs["UP"].rate,
        0.03
    );
    assert_eq!(account.token_fee_configs_fast.load()["UP"].rate, 0.03);
    assert_eq!(account.instance_snapshot("b").unwrap().cash, 50.0);
    account.flush_persistence(Duration::from_secs(3)).unwrap();
    drop(lifecycle);
    drop(cold);
    drop(account);
    let restored = SharedAccount::new_persistent("fees", &path).unwrap();
    assert_eq!(restored.token_fee_configs_fast.load()["UP"].rate, 0.03);
    assert_eq!(restored.instance_snapshot("b").unwrap().cash, 50.0);
    drop(restored);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(persistence_wal_path(&path));
}

#[test]
fn unused_fee_registry_overflow_fails_closed_including_duplicate_retry() {
    let mut account = SharedAccount::new("fee-overflow");
    let (tx, _rx) = crossbeam_channel::bounded(1);
    account.lifecycle_mirror_tx = tx;
    let account = Arc::new(account);
    account.register_instance("a", 1.0);
    account
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    let lifecycle = account.bind_account_lifecycle_owner().unwrap();
    lifecycle.mark_current_thread().unwrap();
    account
        .register_token_fee_config(&["UP".into()], 0.02, 1.0)
        .unwrap();
    assert!(account
        .register_token_fee_config(&["DOWN".into()], 0.02, 1.0)
        .is_err());
    assert!(!account.admission_fast.load(Ordering::Acquire));
    assert!(account
        .register_token_fee_config(&["DOWN".into()], 0.02, 1.0)
        .is_err());
    assert_eq!(
        account
            .lifecycle_mirror_queue_overflows
            .load(Ordering::Acquire),
        1
    );
}

#[test]
fn token_interest_typed_wal_replays_register_retire_prune_without_rewriting_sibling() {
    let _guard = super::tests::persistence_test_guard();
    let path = std::env::temp_dir().join(format!(
        "token-scope-tail-{}-{}.json",
        std::process::id(),
        wall_clock_ms()
    ));
    let account = Arc::new(SharedAccount::new_persistent("typed-interest", &path).unwrap());
    account.register_instance("a", 1.0);
    account.register_instance("b", 1.0);
    account
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    account
        .register_token_interest("a", "ca", "up-a", "down-a")
        .unwrap();
    account
        .register_token_interest("b", "cb", "up-b", "down-b")
        .unwrap();
    account.retire_token_interest("a", "ca");
    account.flush_persistence(Duration::from_secs(3)).unwrap();
    let before = account.instance_snapshot("b").unwrap();
    drop(account);
    let account = Arc::new(SharedAccount::new_persistent("typed-interest", &path).unwrap());
    assert!(account
        .token_interests()
        .iter()
        .find(|i| i.condition_id == "ca")
        .unwrap()
        .retire_after_ms
        .is_some());
    // Bind the real cold/mirror owners so the aggregate cannot be overwritten
    // by a legacy bootstrap refresh while expiring the test interest.
    let (_, owner) = account.bind_account_owner().unwrap();
    owner.mark_current_thread().unwrap();
    let _lifecycle = account.bind_account_lifecycle_owner().unwrap();
    {
        let mut state = account.state.lock().unwrap();
        state
            .instances
            .get_mut("a")
            .unwrap()
            .token_interests
            .get_mut("ca")
            .unwrap()
            .retire_after_ms = Some(0);
    }
    assert_eq!(account.token_interests().len(), 1);
    let generation = account
        .persistence
        .as_ref()
        .unwrap()
        .next_generation
        .load(Ordering::Acquire);
    for _ in 0..4 {
        assert_eq!(account.token_interests().len(), 1);
    }
    assert_eq!(
        account
            .persistence
            .as_ref()
            .unwrap()
            .next_generation
            .load(Ordering::Acquire),
        generation,
        "read-only polls must not enqueue WAL"
    );
    account.flush_persistence(Duration::from_secs(3)).unwrap();
    drop(_lifecycle);
    drop(owner);
    drop(account);
    let restored = SharedAccount::new_persistent("typed-interest", &path).unwrap();
    assert_eq!(
        restored
            .token_interests()
            .iter()
            .map(|i| i.condition_id.as_str())
            .collect::<Vec<_>>(),
        vec!["cb"]
    );
    assert_eq!(restored.instance_snapshot("b").unwrap().cash, before.cash);
    drop(restored);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(persistence_wal_path(&path));
}

#[test]
fn matched_and_confirmed_snapshots_expose_pending_and_close_per_token() {
    let account = SharedAccount::new("pending-snapshot");
    account.register_instance("a", 1.0);
    account
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    account
        .reserve_order("a", "coid", "oid", "UP", Side::Buy, 10.0, 0.5, 0)
        .unwrap();
    account
        .apply_trade_transition(
            "trade",
            "MATCHED",
            "coid",
            "oid",
            "UP",
            Side::Buy,
            10.0,
            0.5,
        )
        .unwrap();
    let snapshot = account.monitoring_snapshot();
    assert_eq!(snapshot.reconciliation.pending_physical_cash, -5.0);
    assert_eq!(
        snapshot.reconciliation.pending_physical_positions["UP"],
        10.0
    );
    assert!(snapshot.reconciliation.cash_delta.abs() < EPS);
    assert!(snapshot.reconciliation.position_delta_abs < EPS);
    account
        .apply_trade_transition("trade", "MINED", "coid", "oid", "UP", Side::Buy, 10.0, 0.5)
        .unwrap();
    let snapshot = account.monitoring_snapshot();
    assert_eq!(snapshot.reconciliation.pending_physical_cash, 0.0);
    assert!(snapshot.reconciliation.cash_delta.abs() < EPS);
    assert!(snapshot.reconciliation.position_delta_abs < EPS);
}

fn mirrored_trade(instance: &str, cash: f64, generation: u64) -> VirtualTradePersistenceDelta {
    VirtualTradePersistenceDelta {
        instance_id: instance.into(),
        cash,
        reserved_cash: 0.0,
        token_id: "UP".into(),
        position: 3.0,
        reserved_position: 0.0,
        client_order_id: "coid".into(),
        order: None,
        trade_key: "trade".into(),
        trade: None,
        fee_attribution_pending: false,
        recovery_pending: false,
        routine_cancel_audit: false,
        ledger_generation: generation,
    }
}

#[test]
fn trade_mirror_publishes_coherent_economics_without_rebuilding_unchanged_registries() {
    let account = Arc::new(SharedAccount::new("mirror-snapshot"));
    account.register_instance("a", 1.0);
    account.register_instance("b", 1.0);
    account
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    let (_, owner) = account.bind_account_owner().unwrap();
    owner.mark_current_thread().unwrap();
    let _lifecycle = account.bind_account_lifecycle_owner().unwrap();
    let ended = account.ended_token_ids_fast.load_full();
    let fees = account.token_fee_configs_fast.load_full();
    let maintenance = Arc::clone(&account.economic_snapshot_fast.load().maintenance_operations);
    for (watermark, cash) in [(1, 48.0), (2, 49.0)] {
        account
            .lifecycle_mirror_tx
            .send(LifecycleMirrorDelta::Trade {
                watermark,
                delta: mirrored_trade("a", cash, watermark),
            })
            .unwrap();
        owner.execute_lifecycle_mirror();
        let snapshot = account.monitoring_snapshot_fast();
        assert_eq!(snapshot.reconciliation.virtual_cash, cash + 50.0);
        assert_eq!(snapshot.reconciliation.ledger_generation, watermark);
        assert!(snapshot.reconciliation.cash_delta.abs() < EPS);
        assert!(snapshot.reconciliation.position_delta_abs < EPS);
        assert!(Arc::ptr_eq(
            &ended,
            &account.ended_token_ids_fast.load_full()
        ));
        assert!(Arc::ptr_eq(
            &fees,
            &account.token_fee_configs_fast.load_full()
        ));
        assert!(Arc::ptr_eq(
            &maintenance,
            &account.economic_snapshot_fast.load().maintenance_operations
        ));
        assert_eq!(account.instance_snapshot("b").unwrap().cash, 50.0);
    }
    // A genuine registry mutation still invalidates its publication.
    account
        .register_token_interest("a", "new-condition", "new-up", "new-down")
        .unwrap();
    account.retire_token_interest("a", "new-condition");
    assert!(account.token_event_has_ended("new-up"));
}

#[test]
#[ignore = "focused cold transaction baseline-copy and mirror publication benchmark"]
fn benchmark_cold_interest_capture_and_mirror_registry() {
    const N: usize = 2000;
    let mut state = SharedAccountState::default();
    state.instances.insert("a".into(), InstanceLedger::new(1.0));
    for i in 0..4000 {
        let token = format!("{i:064}");
        state.settled_token_values.insert(token.clone(), 1.0);
        state.physical_positions.insert(token.clone(), 0.0);
        state
            .instances
            .get_mut("a")
            .unwrap()
            .positions
            .insert(token, 0.0);
    }
    let account = SharedAccount::new("mirror-benchmark");
    account
        .economic_snapshot_fast
        .store(Arc::new(PublishedEconomicSnapshot::from_state(&state)));
    for sparse in [false, true] {
        let mut samples = Vec::with_capacity(N);
        let pending = PendingPhysicalDeltas::default();
        for _ in 0..N {
            let start = Instant::now();
            if sparse {
                recompute_reconciliation_with_pending(&mut state, "benchmark", &pending);
            } else {
                legacy_reconciliation_positions(&mut state, &pending);
            }
            samples.push(start.elapsed().as_nanos());
        }
        samples.sort_unstable();
        eprintln!("reconciliation sparse={sparse} n={N} tokens=4000 p50={} p99={} p999={} max={} unit=ns queue_depth=0 overflow=0 boundary=aggregate_position_residual_recompute", samples[N/2],samples[N*99/100],samples[N*999/1000],samples[N-1]);
    }
    for typed in [false, true] {
        let mut samples = Vec::with_capacity(N);
        let interest = TokenInterest {
            condition_id: "condition".into(),
            instance_id: "a".into(),
            up_token_id: "up".into(),
            down_token_id: "down".into(),
            ..Default::default()
        };
        for _ in 0..N {
            let start = Instant::now();
            if typed {
                let mut changes = Vec::with_capacity(1);
                persistence_wal_set(
                    &mut changes,
                    vec![
                        "instances".into(),
                        "a".into(),
                        "token_interests".into(),
                        "condition".into(),
                    ],
                    &interest,
                )
                .unwrap();
                std::hint::black_box(changes);
            } else {
                let mut capture = ColdPersistenceCapture::default();
                capture.enabled = true;
                capture.before_mutation(&state, true);
                std::hint::black_box(capture.take_job(&state).unwrap());
            }
            samples.push(start.elapsed().as_nanos());
        }
        samples.sort_unstable();
        eprintln!("interest_persist typed={typed} n={N} tokens=4000 p50={} p99={} p999={} max={} unit=ns queue_depth=0 overflow=0 boundary=transaction_capture_create_and_drop_excludes_async_writer", samples[N/2],samples[N*99/100],samples[N*999/1000],samples[N-1]);
    }
    for registry in [true, false] {
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let start = Instant::now();
            account.publish_control_snapshots_scoped(&state, registry);
            if !registry {
                // Include the newly-added coherent economics publication,
                // so the comparison cannot hide its cost in a different lane.
                let previous = account.economic_snapshot_fast.load();
                let next = PublishedEconomicSnapshot::from_state_with_cold(
                    &state,
                    Arc::clone(&previous.physical_positions),
                    Arc::clone(&previous.maintenance_operations),
                    previous.pending_maintenance_operations,
                );
                account.economic_snapshot_fast.store(Arc::new(next));
            }
            samples.push(start.elapsed().as_nanos());
        }
        samples.sort_unstable();
        eprintln!("mirror_publication registry_rebuild={registry} n={N} tokens=4000 p50={} p99={} p999={} max={} unit=ns queue_depth=0 overflow=0",samples[N/2],samples[N*99/100],samples[N*999/1000],samples[N-1]);
    }
}

#[test]
fn sparse_reconciliation_preserves_signed_and_cancelling_instance_positions() {
    for seed in 0..64 {
        let mut state = SharedAccountState::default();
        for owner in ["a", "b"] {
            state
                .instances
                .insert(owner.into(), InstanceLedger::new(1.0));
        }
        for token in 0..100 {
            let key = format!("token-{token}");
            let a = if token % 11 == 0 {
                (seed + token) as f64 - 30.0
            } else {
                0.0
            };
            let b = if token % 7 == 0 { -a } else { 0.0 };
            state
                .instances
                .get_mut("a")
                .unwrap()
                .positions
                .insert(key.clone(), a);
            state
                .instances
                .get_mut("b")
                .unwrap()
                .positions
                .insert(key.clone(), b);
            state
                .physical_positions
                .insert(key, if token % 5 == 0 { 13.0 } else { 0.0 });
        }
        let mut pending = PendingPhysicalDeltas::default();
        pending.positions.insert("token-11".into(), 2.0);
        let mut old = state.clone();
        legacy_reconciliation_positions(&mut old, &pending);
        recompute_reconciliation_with_pending(&mut state, "test", &pending);
        assert_eq!(state.unallocated_positions, old.unallocated_positions);
    }
}

// Exact old token union/hash loop, retained only for semantic and latency comparison.
fn legacy_reconciliation_positions(
    state: &mut SharedAccountState,
    pending: &PendingPhysicalDeltas,
) {
    state.unallocated_positions.clear();
    let mut tokens: HashSet<&str> = state
        .physical_positions
        .keys()
        .map(String::as_str)
        .collect();
    tokens.extend(
        state
            .instances
            .values()
            .flat_map(|instance| instance.positions.keys().map(String::as_str)),
    );
    for token in tokens {
        let physical = state.physical_positions.get(token).copied().unwrap_or(0.0);
        let virtual_qty: f64 = state
            .instances
            .values()
            .map(|instance| instance.positions.get(token).copied().unwrap_or(0.0))
            .sum();
        let expected = virtual_qty - pending.positions.get(token).copied().unwrap_or(0.0);
        let delta = physical - expected;
        if delta.abs() > reconciliation_tolerance(physical, expected) {
            state.unallocated_positions.insert(token.into(), delta);
        }
    }
}
