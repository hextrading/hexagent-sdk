use super::*;

fn setup() -> Arc<SharedAccount> {
    let account = Arc::new(SharedAccount::new("coherent-startup-seed"));
    account.register_instance("owner", 1.0);
    account.register_instance("sibling", 1.0);
    account
        .apply_physical_snapshot(200.0, HashMap::new())
        .unwrap();
    account
        .reserve_order("owner", "order", "oid", "UP", Side::Buy, 10.0, 0.5, 700)
        .unwrap();
    assert!(account.capture_instance_startup_seed("owner").is_some());
    account
}

fn trade(account: &SharedAccount, status: &str) {
    assert!(!matches!(
        account.apply_trade_transition_with_context(
            "trade",
            status,
            "order",
            "oid",
            "UP",
            Side::Buy,
            10.0,
            0.5,
            true,
            100,
        ),
        TradeTransitionResult::Rejected
    ));
}

#[test]
fn startup_seed_does_not_mix_live_balance_with_lagging_mirror_replay_rows() {
    let account = setup();
    let (_, cold) = account.bind_account_owner().unwrap();
    let lifecycle = account.bind_account_lifecycle_owner().unwrap();
    lifecycle.mark_current_thread().unwrap();
    let before = account.capture_instance_startup_seed("owner").unwrap();
    trade(&account, "MATCHED");
    assert_eq!(account.instance_snapshot("owner").unwrap().cash, 95.0);
    let lagging = account.capture_instance_startup_seed("owner").unwrap();
    assert_eq!(lagging.snapshot, before.snapshot);
    assert!(lagging.restored_trades.is_empty());
    assert!(account.lifecycle_mirror_queue_metrics().0 > 0);
    cold.mark_current_thread().unwrap();
    cold.execute_lifecycle_mirror();
    let after = account.capture_instance_startup_seed("owner").unwrap();
    assert_eq!(after.snapshot.cash, 95.0);
    assert_eq!(after.snapshot.positions["UP"], 10.0);
    assert_eq!(after.snapshot.reserved_cash, 0.0);
    assert_eq!(after.restored_trades.len(), 1);
    assert!(after.restored_trades[0].booked);
    assert!(after.restored_trades[0].ledger_generation <= after.snapshot.ledger_generation);
    assert!(after.snapshot.ledger_generation > before.snapshot.ledger_generation);
    assert_eq!(
        before.snapshot.cash, 100.0,
        "published seed remains immutable"
    );
    let sibling = account.capture_instance_startup_seed("sibling").unwrap();
    assert_eq!(sibling.snapshot.cash, 100.0);
    assert!(sibling.orders.is_empty());
    assert!(sibling.restored_trades.is_empty());
    assert!(account.capture_instance_startup_seed("unknown").is_none());
}

#[test]
fn startup_seed_duplicate_and_failed_replay_keep_balance_and_tombstone_paired() {
    let account = setup();
    let (_, cold) = account.bind_account_owner().unwrap();
    let lifecycle = account.bind_account_lifecycle_owner().unwrap();
    lifecycle.mark_current_thread().unwrap();
    cold.mark_current_thread().unwrap();
    trade(&account, "MATCHED");
    cold.execute_lifecycle_mirror();
    let matched = account.capture_instance_startup_seed("owner").unwrap();
    trade(&account, "MATCHED");
    cold.execute_lifecycle_mirror();
    let duplicate = account.capture_instance_startup_seed("owner").unwrap();
    assert_eq!(duplicate.snapshot, matched.snapshot);
    assert_eq!(duplicate.restored_trades, matched.restored_trades);
    trade(&account, "FAILED");
    cold.execute_lifecycle_mirror();
    let failed = account.capture_instance_startup_seed("owner").unwrap();
    assert_eq!(failed.snapshot.cash, 100.0);
    assert_eq!(failed.snapshot.positions["UP"], 0.0);
    assert_eq!(failed.restored_trades.len(), 1);
    assert!(!failed.restored_trades[0].booked);
    assert!(failed.restored_trades[0].ledger_generation <= failed.snapshot.ledger_generation);
    trade(&account, "MATCHED");
    cold.execute_lifecycle_mirror();
    let stale = account.capture_instance_startup_seed("owner").unwrap();
    assert_eq!(stale.snapshot, failed.snapshot);
    assert_eq!(stale.restored_trades, failed.restored_trades);
}

#[test]
fn startup_seed_requires_snapshot_and_keeps_mirror_fault_fail_closed() {
    let unseeded = SharedAccount::new("unseeded");
    unseeded.register_instance("owner", 1.0);
    assert!(unseeded.capture_instance_startup_seed("owner").is_none());
    let account = setup();
    account
        .lifecycle_mirror_incident_active
        .store(true, Ordering::Release);
    let seed = account.capture_instance_startup_seed("owner").unwrap();
    assert!(seed.uncertain);
    assert!(!seed.passive_admission_allowed);
    assert!(!seed.fee_degraded_only);
    assert!(seed.uncertain_reason.unwrap().contains("mirror"));
}

#[test]
fn startup_seed_pairs_confirmed_split_identity_with_its_owner_balance() {
    let account = setup();
    account.reserve_maintenance_operation("seed-split", MaintenanceOperationKind::Split,
        "condition", "UP", "DOWN", &[("owner".into(), 40.0)].into()).unwrap();
    let pending = account.capture_instance_startup_seed("owner").unwrap();
    assert!(pending.confirmed_split_conditions.is_empty());
    account.mark_maintenance_operation_submitted("seed-split", "relayer-job").unwrap();
    account.confirm_maintenance_operation("seed-split").unwrap();
    let confirmed = account.capture_instance_startup_seed("owner").unwrap();
    assert!(confirmed.confirmed_split_conditions.contains("condition"));
    assert_eq!(confirmed.snapshot.positions["UP"], 40.0);
    assert_eq!(confirmed.snapshot.cash, 60.0);
    assert!(account.capture_instance_startup_seed("sibling").unwrap().confirmed_split_conditions.is_empty());
}
