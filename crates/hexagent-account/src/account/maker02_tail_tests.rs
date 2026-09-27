// Included in shared_account::tests to reuse its exact persistent-ledger helpers.

#[test]
fn maintenance_scoped_wal_matches_full_diff_and_preserves_later_private_rows() {
    let account = seeded_account();
    let tokens = HashSet::from(["UP".into(), "DOWN".into()]);
    let allocations = HashMap::from([("a".into(), 2.0)]);
    for (id, action) in [("split", 0), ("split", 1), ("merge", 2), ("merge", 3)] {
        let before = account.state.lock().unwrap().clone();
        let view = maintenance_persistence_view(&before, id, &tokens).unwrap();
        match action {
            0 => account
                .reserve_maintenance_operation(
                    id,
                    MaintenanceOperationKind::Split,
                    "c",
                    "UP",
                    "DOWN",
                    &allocations,
                )
                .unwrap(),
            1 => {
                account
                    .mark_maintenance_operation_submitted(id, "tx")
                    .unwrap();
                account.confirm_maintenance_operation(id).unwrap();
            }
            2 => account
                .reserve_maintenance_operation(
                    id,
                    MaintenanceOperationKind::Merge,
                    "c",
                    "UP",
                    "DOWN",
                    &allocations,
                )
                .unwrap(),
            _ => account.fail_maintenance_operation(id, "test failure"),
        }
        let after = account.state.lock().unwrap().clone();
        let changes = persistence_cold_transaction_diff(
            &view,
            &maintenance_persistence_view(&after, id, &tokens).unwrap(),
        );
        let mut projected = serde_json::to_value(&before).unwrap();
        for change in changes.clone() {
            apply_persistence_wal_change(&mut projected, change).unwrap();
        }
        // Guard entry can import zero-valued derived reservation/position
        // slots before the transaction baseline. Absent and zero are identical
        // balances; the scoped WAL deliberately does not publish those imports.
        let normalize = |value: &mut serde_json::Value| {
            for field in ["physical_positions", "unallocated_positions"] {
                value[field]
                    .as_object_mut()
                    .unwrap()
                    .retain(|_, v| v.as_f64() != Some(0.0));
            }
            for instance in value["instances"].as_object_mut().unwrap().values_mut() {
                for field in [
                    "positions",
                    "reserved_positions",
                    "maintenance_reserved_positions",
                ] {
                    instance[field]
                        .as_object_mut()
                        .unwrap()
                        .retain(|_, v| v.as_f64() != Some(0.0));
                }
            }
        };
        let mut expected = serde_json::to_value(&after).unwrap();
        normalize(&mut projected);
        normalize(&mut expected);
        assert!(
            projected == expected,
            "action {action} missing changes: {:?}",
            persistence_json_diff(&projected, &expected)
        );
        // Cold reservations cannot replace lifecycle maps/unchanged balance
        // leaves that the writer may have received after the cold snapshot.
        let mut newer = serde_json::to_value(&before).unwrap();
        newer["orders"]["later-owner-b"] = serde_json::json!({"marker": 77});
        newer["instances"]["b"]["cash"] = serde_json::json!(333.0);
        for change in changes {
            apply_persistence_wal_change(&mut newer, change).unwrap();
        }
        assert_eq!(newer["orders"]["later-owner-b"]["marker"], 77);
        assert_eq!(newer["instances"]["b"]["cash"], 333.0);
        // Duplicate terminal notifications are no-ops.
        if action == 1 {
            account.confirm_maintenance_operation(id).unwrap();
        }
        if action == 3 {
            account.fail_maintenance_operation(id, "duplicate");
        }
    }
}

#[test]
#[ignore = "release: cold maintenance with live-sized retained history and real WAL writer"]
fn benchmark_live_maintenance_transaction() {
    let _guard = persistence_test_guard();
    let path = std::env::temp_dir().join(format!(
        "hexagent-maintenance-tail-{}.json",
        std::process::id()
    ));
    remove_persistence_test_files(&path);
    let seed = seeded_account();
    let mut state = (*seed.lock_state()).clone();
    // The production ledger's dominant history is retired trade ownership.
    seed.reserve_order(
        "a",
        "template",
        "oid-template",
        "UP",
        Side::Buy,
        1.0,
        0.5,
        0,
    )
    .unwrap();
    seed.apply_trade_transition_with_context(
        "template-trade",
        "CONFIRMED",
        "template",
        "oid-template",
        "UP",
        Side::Buy,
        1.0,
        0.5,
        true,
        100,
    );
    let template = seed.trade_ownership("template-trade").unwrap();
    for i in 0..80_000 {
        let mut trade = template.clone();
        trade.trade_key = format!("retired-{i:064x}");
        state.retired_trade_ownership_tombstones.insert(
            trade.trade_key.clone(),
            RetiredTradeOwnershipTombstone {
                ownership: trade,
                execution_pricing: None,
                is_maker: Some(true),
                authenticated_terminal_noop: false,
                retired_at_ms: 1,
            },
        );
    }
    let persisted = PersistedAccount {
        version: PERSISTENCE_VERSION,
        account_id: "acct".into(),
        persistence_generation: 0,
        state,
    };
    std::fs::write(&path, serde_json::to_vec(&persisted).unwrap()).unwrap();
    drop(seed);
    let account = std::sync::Arc::new(SharedAccount::new_persistent("acct", &path).unwrap());
    // Bind the real cold lane so the measured path does not use pre-bind
    // compatibility publication of all lifecycle maps.
    let (_, owner) = account.bind_account_owner().unwrap();
    let _lifecycle_owner = account.bind_account_lifecycle_owner().unwrap();
    owner.mark_current_thread().unwrap();
    let n = std::env::var("HEXAGENT_TAIL_SAMPLES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    let mut samples = Vec::with_capacity(n);
    let mut drained = Vec::with_capacity(n);
    let allocations = HashMap::from([("a".to_owned(), 1.0)]);
    for i in 0..n + 16 {
        let id = format!("maintenance-tail-{i}");
        let start = Instant::now();
        account
            .reserve_maintenance_operation_inner(
                &id,
                MaintenanceOperationKind::Split,
                "c",
                "UP",
                "DOWN",
                &allocations,
                false,
            )
            .unwrap();
        let elapsed = start.elapsed().as_nanos() as u64;
        account.flush_persistence(Duration::from_secs(10)).unwrap();
        let persisted = start.elapsed().as_nanos() as u64;
        account.fail_maintenance_operation(&id, "benchmark releases reservation");
        account.flush_persistence(Duration::from_secs(10)).unwrap();
        owner.execute_lifecycle_mirror();
        if i >= 16 {
            samples.push(elapsed);
            drained.push(persisted);
        }
    }
    for (boundary, values) in [
        ("maintenance_reserve_including_publication", &mut samples),
        ("reserve_through_WAL_flush", &mut drained),
    ] {
        values.sort_unstable();
        let q = |p: usize| values[(n * p).div_ceil(1000) - 1];
        eprintln!("live_maintenance_probe boundary={boundary} N={n} history=80000 p50_ns={} p99_ns={} p999_ns={} max_ns={}",q(500),q(990),q(999),q(1000));
    }
    eprintln!(
        "live_maintenance_probe persistence_metrics={:?} mirror_metrics={:?}",
        account.persistence.as_ref().unwrap().metrics(),
        account.lifecycle_mirror_queue_metrics()
    );
    drop(owner);
    drop(_lifecycle_owner);
    drop(account);
    remove_persistence_test_files(&path);
}
