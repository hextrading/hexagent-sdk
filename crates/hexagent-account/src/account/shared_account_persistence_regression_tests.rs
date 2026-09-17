use super::*;

#[test]
fn zero_fill_recovery_revalidates_on_lifecycle_owner_before_releasing_reservation() {
    let account = Arc::new(SharedAccount::new("zero-fill-owner"));
    for iid in ["a", "b"] {
        account.register_instance(iid, 1.0);
    }
    account
        .apply_physical_snapshot(200.0, HashMap::from([("UP".into(), 80.0)]))
        .unwrap();
    for (iid, coid) in [("a", "a-order"), ("b", "b-order")] {
        account
            .reserve_order(iid, coid, coid, "UP", Side::Sell, 20.0, 0.5, 0)
            .unwrap();
        account.mark_order_status(coid, OrderStatus::NewOrderTimeout);
    }
    let a = account.order("a-order").unwrap();
    let b = account.order("b-order").unwrap();
    let (_, cold_owner) = account.bind_account_owner().unwrap();
    cold_owner.mark_current_thread().unwrap();
    let owner = account.bind_account_lifecycle_owner().unwrap();
    let (stop_tx, stop_rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        owner.mark_current_thread().unwrap();
        loop {
            crossbeam_channel::select_biased! {
                recv(owner.receiver()) -> command => match command {
                    Ok(command) => owner.execute(command), Err(_) => break,
                },
                recv(stop_rx) -> _ => break,
            }
        }
    });
    account
        .apply_authoritative_order_audit(
            "a-order",
            OrderStatus::Cancelled,
            &AuthoritativeOrderAudit {
                original_size: Some("20".into()),
                size_matched: Some("4".into()),
                associate_trades: vec!["late-trade".into()],
            },
        )
        .unwrap();
    cold_owner.execute_lifecycle_mirror();
    let matched = account.order("a-order").unwrap();
    assert!(account.apply_zero_fill_recovery(&a).is_err());
    assert_eq!(account.order("a-order").unwrap(), matched);
    let mut wrong_owner = b.clone();
    wrong_owner.instance_id = "a".into();
    assert!(account.apply_zero_fill_recovery(&wrong_owner).is_err());
    assert_eq!(account.order("b-order").unwrap().reserved_quantity, 20.0);
    assert_eq!(
        account.apply_zero_fill_recovery(&b).unwrap(),
        FillAuditPendingTransition::Resolved
    );
    cold_owner.execute_lifecycle_mirror();
    let terminal = account.order("b-order").unwrap();
    assert_eq!(terminal.status, OrderStatus::Cancelled);
    assert_eq!(terminal.reserved_quantity, 0.0);
    assert!(terminal.terminal_trade_ids_authoritative);
    assert_eq!(
        account.apply_zero_fill_recovery(&b).unwrap(),
        FillAuditPendingTransition::Resolved
    );
    assert_eq!(account.order("b-order").unwrap(), terminal);
    assert_eq!(account.order("a-order").unwrap(), matched);
    assert_eq!(account.account_lifecycle_queue_metrics().2, 0);
    stop_tx.send(()).unwrap();
    worker.join().unwrap();
}

#[test]
#[ignore = "focused no-fill recovery owner dispatch/commit microbenchmark"]
fn benchmark_zero_fill_recovery_owner_commit() {
    let account = Arc::new(SharedAccount::new("zero-fill-benchmark"));
    account.register_instance("a", 1.0);
    account
        .apply_physical_snapshot(1_000_000.0, HashMap::new())
        .unwrap();
    let count = 300;
    let mut candidates = Vec::new();
    for i in 0..(count * 2) {
        let coid = format!("a-{i}");
        account
            .reserve_order("a", &coid, &coid, "UP", Side::Buy, 20.0, 0.5, 0)
            .unwrap();
        candidates.push(account.order(&coid).unwrap());
    }
    let owner = account.bind_account_lifecycle_owner().unwrap();
    let (stop_tx, stop_rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        owner.mark_current_thread().unwrap();
        loop {
            crossbeam_channel::select_biased! {
                recv(owner.receiver()) -> command => match command {
                    Ok(command) => owner.execute(command), Err(_) => break,
                },
                recv(stop_rx) -> _ => break,
            }
        }
    });
    let audit = AuthoritativeOrderAudit {
        original_size: Some("20".into()),
        size_matched: Some("0".into()),
        associate_trades: vec![],
    };
    for (mode, orders) in [
        ("previous_unconditional", &candidates[..count]),
        ("conditional_owner_commit", &candidates[count..]),
    ] {
        let mut samples = Vec::new();
        for order in orders {
            let start = std::time::Instant::now();
            let result = if mode == "previous_unconditional" {
                account.apply_authoritative_order_audit(
                    &order.client_order_id,
                    OrderStatus::Cancelled,
                    &audit,
                )
            } else {
                account.apply_zero_fill_recovery(order)
            };
            samples.push(start.elapsed().as_nanos() as u64);
            assert_eq!(result.unwrap(), FillAuditPendingTransition::Resolved);
        }
        samples.sort_unstable();
        let q = |p: f64| samples[((samples.len() as f64 * p).ceil() as usize).saturating_sub(1)];
        let queue = account.account_lifecycle_queue_metrics();
        eprintln!("zero_fill boundary=owner_dispatch+conditional_check+account_commit+reply mode={mode} n={count} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth={} queue_high_water={} overflow={} network_and_disk=false", q(0.5), q(0.99), q(0.999), samples.last().unwrap(), queue.0, queue.1, queue.2);
    }
    stop_tx.send(()).unwrap();
    worker.join().unwrap();
}

fn raw_durable(path: &Path) -> SharedAccountState {
    let mut persisted: PersistedAccount =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    replay_persistence_wal(path, &mut persisted).unwrap();
    persisted.state
}

#[test]
fn cold_checkpoint_cannot_reopen_cancelled_order_while_mirror_is_parked() {
    let _serial = tests::persistence_test_guard();
    let path = std::env::temp_dir().join(format!(
        "hexagent-cancel-cold-snapshot-{}.json",
        std::process::id()
    ));
    tests::remove_persistence_test_files(&path);
    let account = Arc::new(SharedAccount::new_persistent("cancel-cold-snapshot", &path).unwrap());
    for iid in ["a", "b"] {
        account.register_instance(iid, 1.0);
    }
    account
        .apply_physical_snapshot(200.0, HashMap::from([("UP".into(), 80.0)]))
        .unwrap();
    for (iid, coid, qty) in [("a", "a-cancel", 20.0), ("b", "b-live", 10.0)] {
        account
            .reserve_order(iid, coid, coid, "UP", Side::Sell, qty, 0.5, 0)
            .unwrap();
        account.mark_order_status(coid, OrderStatus::Accepted);
    }
    let (_handle, cold_owner) = account.bind_account_owner().unwrap();
    cold_owner.mark_current_thread().unwrap();
    let lifecycle_owner = account.bind_account_lifecycle_owner().unwrap();
    let (ready_tx, ready_rx) = crossbeam_channel::bounded(1);
    let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(1);
    let lifecycle_account = account.clone();
    let worker = std::thread::spawn(move || {
        lifecycle_owner.mark_current_thread().unwrap();
        lifecycle_account
            .apply_authoritative_order_audit(
                "a-cancel",
                OrderStatus::Cancelled,
                &AuthoritativeOrderAudit {
                    original_size: Some("20".into()),
                    size_matched: Some("0".into()),
                    associate_trades: vec![],
                },
            )
            .unwrap();
        ready_tx.send(()).unwrap();
        let _ = stop_rx.recv();
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    account.flush_persistence(Duration::from_secs(5)).unwrap();
    let terminal = raw_durable(&path);
    assert_eq!(terminal.orders["a-cancel"].status, OrderStatus::Cancelled);
    assert_eq!(terminal.instances["a"].reserved_positions["UP"], 0.0);
    assert_eq!(
        account.read_cold_state(|s| s.orders["a-cancel"].status),
        OrderStatus::Accepted
    );
    account.record_gap_replay_pages(3);
    account.record_maintenance_queue_wait(Duration::from_millis(4));
    for generation in 1..=2 {
        // Exercise the general cold transaction fallback as well as the
        // narrow checkpoint path, with an intentionally stale order mirror.
        account.set_risk_blocker("test-cold", format!("checkpoint-{generation}"));
        account.flush_persistence(Duration::from_secs(5)).unwrap();
        assert_eq!(
            raw_durable(&path).orders["a-cancel"],
            terminal.orders["a-cancel"]
        );
        account
            .record_sidecar_checkpoint(
                "a",
                DurableSidecarCheckpoint {
                    generation,
                    expected_entries: 1,
                    recovery_payload: format!("{{\"generation\":{generation}}}"),
                },
            )
            .unwrap();
        account.flush_persistence(Duration::from_secs(5)).unwrap();
        let durable = raw_durable(&path);
        assert_eq!(
            durable.orders["a-cancel"], terminal.orders["a-cancel"],
            "cold checkpoint must not replace newer lifecycle state"
        );
        assert_eq!(durable.instances["a"].reserved_positions["UP"], 0.0);
        assert_eq!(durable.orders["b-live"], terminal.orders["b-live"]);
        assert_eq!(durable.instances["b"].reserved_positions["UP"], 10.0);
        assert_eq!(durable.sidecar_checkpoints["a"].generation, generation);
        assert_eq!(durable.gap_replay_total_pages, 3);
        assert_eq!(durable.maintenance_queue_last_wait_ms, 4);
        assert!(account.clear_risk_blocker("test-cold"));
    }
    cold_owner.execute_lifecycle_mirror();
    assert_eq!(
        account.read_cold_state(|s| s.orders["a-cancel"].status),
        OrderStatus::Cancelled
    );
    stop_tx.send(()).unwrap();
    worker.join().unwrap();
    drop(cold_owner);
    drop(account);
    // Check actual restart as well as every flushed raw prefix; startup repair
    // must not be what makes a previously invalid prefix appear correct.
    let restored = SharedAccount::new_persistent("cancel-cold-snapshot", &path).unwrap();
    assert_eq!(
        restored.order("a-cancel").unwrap().status,
        OrderStatus::Cancelled
    );
    assert_eq!(restored.order("a-cancel").unwrap().reserved_quantity, 0.0);
    drop(restored);
    tests::remove_persistence_test_files(&path);
}

#[test]
fn late_http_cancel_keeps_authoritative_audit_and_pending_trade_obligation() {
    for side in [Side::Buy, Side::Sell] {
        for matched in [0.0, 4.0] {
            for http_first in [false, true] {
                let account = tests::seeded_account();
                account
                    .reserve_order("a", "a-cancel", "oid-a", "UP", side, 10.0, 0.5, 0)
                    .unwrap();
                account
                    .reserve_order("b", "b-live", "oid-b", "UP", Side::Buy, 2.0, 0.5, 0)
                    .unwrap();
                let sibling = account.order("b-live").unwrap();
                if http_first {
                    assert!(account.mark_cancelled_pending_audit("a-cancel"));
                }
                account
                    .apply_authoritative_order_audit(
                        "a-cancel",
                        OrderStatus::Cancelled,
                        &AuthoritativeOrderAudit {
                            original_size: Some("10".into()),
                            size_matched: Some(matched.to_string()),
                            associate_trades: if matched > 0.0 {
                                vec!["trade-a".into()]
                            } else {
                                vec![]
                            },
                        },
                    )
                    .unwrap();
                let authoritative = account.order("a-cancel").unwrap();
                let pending = account.pending_order_audit_ids();
                let generation = account.order_audit_generation();
                for _ in 0..3 {
                    account.mark_cancelled_pending_audit("a-cancel");
                    assert_eq!(
                        account.order("a-cancel").unwrap(),
                        authoritative,
                        "late/duplicate HTTP must preserve the complete audit"
                    );
                    assert_eq!(account.pending_order_audit_ids(), pending);
                    assert_eq!(account.order_audit_generation(), generation);
                    assert_eq!(account.order("b-live").unwrap(), sibling);
                }
                if matched > 0.0 {
                    account
                        .apply_trade_transition(
                            "trade-a", "MATCHED", "a-cancel", "oid-a", "UP", side, matched, 0.5,
                        )
                        .unwrap();
                    account
                        .apply_trade_transition(
                            "trade-a",
                            "CONFIRMED",
                            "a-cancel",
                            "oid-a",
                            "UP",
                            side,
                            matched,
                            0.5,
                        )
                        .unwrap();
                    let complete = account.order("a-cancel").unwrap();
                    assert_eq!(complete.filled_quantity, matched);
                    assert_eq!(complete.status, OrderStatus::Cancelled);
                    assert_eq!(
                        (complete.reserved_cash, complete.reserved_quantity),
                        (0.0, 0.0)
                    );
                }
            }
        }
    }
}

#[test]
fn cold_transaction_preserves_concurrent_audit_members_and_trade_fields() {
    let before = serde_json::json!({
        "orders": {"a": {"status": "accepted", "reserved_quantity": 20}},
        "trades": {"trade-b": {"failure_reconciled": false, "status": "FAILED"}},
        "recovery_pending_orders": ["old-a"], "routine_cancel_audits": ["old-a"],
        "fee_attribution_pending": [], "startup_query_repair_orders": [],
        "sidecar_checkpoints": {},
    });
    let mut after = before.clone();
    after["recovery_pending_orders"] = serde_json::json!(["new-a"]);
    after["routine_cancel_audits"] = serde_json::json!([]);
    after["trades"]["trade-b"]["failure_reconciled"] = serde_json::json!(true);
    after["sidecar_checkpoints"]["a"] = serde_json::json!({"generation": 2});
    let mut durable = before.clone();
    durable["orders"]["a"] = serde_json::json!({"status": "cancelled", "reserved_quantity": 0});
    durable["orders"]["b"] = serde_json::json!({"status": "accepted", "reserved_quantity": 10});
    durable["recovery_pending_orders"] = serde_json::json!(["old-a", "b"]);
    durable["routine_cancel_audits"] = serde_json::json!(["old-a", "b"]);
    durable["trades"]["trade-b"]["status"] = serde_json::json!("CONFIRMED");
    let changes = persistence_cold_transaction_diff(&before, &after);
    for _ in 0..2 {
        apply_persistence_wal_changes_transactional(&mut durable, &changes).unwrap();
        assert_eq!(durable["orders"]["a"]["status"], "cancelled");
        assert_eq!(durable["orders"]["b"]["reserved_quantity"], 10);
        assert_eq!(durable["trades"]["trade-b"]["status"], "CONFIRMED");
        assert_eq!(durable["trades"]["trade-b"]["failure_reconciled"], true);
        assert_eq!(
            durable["recovery_pending_orders"],
            serde_json::json!(["b", "new-a"])
        );
        assert_eq!(durable["routine_cancel_audits"], serde_json::json!(["b"]));
    }
}

#[test]
fn failed_typed_capture_fails_closed_without_scheduling_stale_snapshot() {
    let _serial = tests::persistence_test_guard();
    let path = std::env::temp_dir().join(format!(
        "hexagent-capture-failure-{}.json",
        std::process::id()
    ));
    tests::remove_persistence_test_files(&path);
    let account = SharedAccount::new_persistent("capture-failure", &path).unwrap();
    account.register_instance("a", 1.0);
    account
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    account.flush_persistence(Duration::from_secs(5)).unwrap();
    assert!(account.admission_fast.load(Ordering::Acquire));
    let before = account.persistence.as_ref().unwrap().scheduled_generation();
    let state = account.lock_state();
    account.schedule_typed_persist(&state, Err("injected serialization failure".into()));
    drop(state);
    // A later healthy-looking mirror or fee-degraded publication must not
    // reopen admission after a missing persistence generation/capture.
    account.record_gap_replay_pages(1);
    account.mark_virtual_fee_pending();
    let persistence = account.persistence.as_ref().unwrap();
    assert_eq!(persistence.scheduled_generation(), before);
    assert!(persistence
        .last_error()
        .unwrap()
        .contains("injected serialization failure"));
    assert!(!account.admission_fast.load(Ordering::Acquire));
    assert!(!account.passive_admission_fast.load(Ordering::Acquire));
    assert!(account.uncertain_fast.load(Ordering::Acquire));
    drop(account);
    tests::remove_persistence_test_files(&path);
}

#[test]
#[ignore = "focused cold persistence capture/materialize/apply benchmark; no disk I/O"]
fn benchmark_cold_persistence_transaction_and_checkpoint() {
    const EVENTS: usize = 300;
    const ORDERS: usize = 1_000;
    let account = tests::seeded_account();
    let mut state = account.lock_state().clone();
    let order = account
        .reserve_order("a", "template", "oid", "UP", Side::Buy, 1.0, 0.5, 0)
        .unwrap();
    for index in 0..ORDERS {
        let coid = format!("history-{index}");
        let mut historical = order.clone();
        historical.client_order_id = coid.clone();
        state.orders.insert(coid, historical);
    }
    let baseline = serde_json::to_value(&state).unwrap();
    let bytes = serde_json::to_vec(&baseline).unwrap().len();
    for mode in [
        "legacy_full_snapshot",
        "cold_transaction",
        "typed_checkpoint",
    ] {
        let mut owned = state.clone();
        let mut durable = baseline.clone();
        let mut samples = Vec::with_capacity(EVENTS);
        for generation in 1..=EVENTS as u64 {
            let checkpoint = DurableSidecarCheckpoint {
                generation,
                expected_entries: 1,
                recovery_payload: format!("{{\"generation\":{generation}}}"),
            };
            let started = Instant::now();
            let before = (mode == "cold_transaction").then(|| Arc::new(owned.clone()));
            owned.sidecar_checkpoints.insert("a".into(), checkpoint);
            let changes = match mode {
                "legacy_full_snapshot" => {
                    let snapshot = owned.clone();
                    // The old writer cloned retry_jobs before serialization.
                    let retry = snapshot.clone();
                    let next = serde_json::to_value(&snapshot).unwrap();
                    let changes = persistence_json_diff(&durable, &next);
                    std::hint::black_box(retry);
                    changes
                }
                "cold_transaction" => materialize_persistence_job(&PersistenceJob {
                    generation,
                    payload: PersistenceJobPayload::ColdTransaction {
                        before: before.unwrap(),
                        after: Arc::new(owned.clone()),
                    },
                })
                .unwrap(),
                _ => materialize_control_entry(
                    &owned,
                    "sidecar_checkpoints",
                    "a",
                    owned.sidecar_checkpoints.get("a"),
                )
                .unwrap(),
            };
            let undo = apply_persistence_wal_changes_transactional(&mut durable, &changes).unwrap();
            drop(undo);
            samples.push(started.elapsed().as_nanos() as u64);
        }
        samples.sort_unstable();
        let percentile =
            |fraction: f64| samples[((EVENTS as f64 * fraction).ceil() as usize).saturating_sub(1)];
        eprintln!("cold_persistence boundary=capture+materialize+apply_without_io mode={mode} n={EVENTS} historical_orders={ORDERS} baseline_bytes={bytes} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0",
            percentile(0.5), percentile(0.99), percentile(0.999), samples[EVENTS-1]);
        assert_eq!(
            durable["sidecar_checkpoints"]["a"]["generation"],
            EVENTS as u64
        );
        assert_eq!(durable["orders"], baseline["orders"]);
    }
}
