use super::*;

fn seeded(version: FeeSettlement, rate: f64) -> SharedAccount {
    let account = SharedAccount::new("v2-fees");
    setup(&account, version, rate);
    account
}

fn setup(account: &SharedAccount, version: FeeSettlement, rate: f64) {
    account.register_instance("owner", 1.0);
    account.register_instance("sibling", 1.0);
    account
        .apply_physical_snapshot(200.0, HashMap::from([("UP".into(), 40.0)]))
        .unwrap();
    account
        .register_token_fee_config_with_settlement(&["UP".into()], rate, 1.0, version)
        .unwrap();
    account
        .reserve_order("owner", "order", "oid", "UP", Side::Buy, 14.0, 0.85, 700)
        .unwrap();
}

fn replay(account: &SharedAccount, status: &str) -> TradeTransitionResult {
    account.apply_trade_transition_with_context(
        "trade",
        status,
        "order",
        "oid",
        "UP",
        Side::Buy,
        14.0,
        0.85,
        false,
        100,
    )
}

fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "actual={actual} expected={expected}"
    );
}

#[test]
fn v2_buy_books_full_shares_cash_fee_and_duplicate_confirmed_isolated() {
    let account = seeded(FeeSettlement::CollateralV2, 0.07);
    let reservation = account.order("order").unwrap();
    close(reservation.reservation_cash_per_share(), 0.85 * 1.07);
    close(reservation.reserved_cash, 14.0 * 0.85 * 1.07);
    for status in [
        "MATCHED",
        "MATCHED",
        "MINED",
        "CONFIRMED",
        "CONFIRMED",
        "FAILED",
    ] {
        assert!(!matches!(
            replay(&account, status),
            TradeTransitionResult::Rejected
        ));
        let owner = account.instance_snapshot("owner").unwrap();
        close(owner.cash, 100.0 - 11.9 - 0.12495);
        close(owner.positions["UP"], 34.0);
        let sibling = account.instance_snapshot("sibling").unwrap();
        close(sibling.cash, 100.0);
        close(sibling.positions["UP"], 20.0);
        let rows = account.restored_trades();
        assert_eq!(rows[0].fee_settlement, FeeSettlement::CollateralV2);
        close(rows[0].usdc_fee, 0.12495);
        close(rows[0].shares_fee, 0.0);
        assert!(validate_persisted_state("v2-fees", &account.lock_state()).is_ok());
    }
}

#[test]
fn v2_failed_reverses_original_cash_fee_after_metadata_revision() {
    let account = seeded(FeeSettlement::CollateralV2, 0.07);
    assert!(matches!(
        replay(&account, "MATCHED"),
        TradeTransitionResult::Applied(_)
    ));
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.12,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    assert!(matches!(
        replay(&account, "FAILED"),
        TradeTransitionResult::Applied(_)
    ));
    for status in ["FAILED", "MATCHED", "CONFIRMED"] {
        assert!(matches!(
            replay(&account, status),
            TradeTransitionResult::OwnedNoop(_)
        ));
        let owner = account.instance_snapshot("owner").unwrap();
        close(owner.cash, 100.0);
        close(owner.positions["UP"], 20.0);
    }
    let state = account.lock_state();
    close(state.trades["trade"].usdc_fee, 0.12495);
    close(state.trades["trade"].fee_config.unwrap().rate, 0.07);
    assert!(validate_persisted_state("v2-fees", &state).is_ok());
}

#[test]
fn legacy_missing_fields_freeze_before_v2_registration_without_repricing() {
    let account = seeded(FeeSettlement::LegacyV1, 0.07);
    replay(&account, "MATCHED");
    // Decode the exact old JSON shape, then install it through the same owner
    // transaction used by startup, retaining its original registry.
    {
        let mut state = account.lock_state();
        let mut value = serde_json::to_value(&state.trades["trade"]).unwrap();
        value.as_object_mut().unwrap().remove("fee_settlement");
        value.as_object_mut().unwrap().remove("fee_config");
        let old: AppliedTrade = serde_json::from_value(value).unwrap();
        assert_eq!(old.fee_settlement, Some(FeeSettlement::LegacyV1));
        state.trades.insert("trade".into(), old);
    }
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.09,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    let owner = account.instance_snapshot("owner").unwrap();
    close(owner.cash, 88.1);
    close(owner.positions["UP"], 33.853);
    assert!(matches!(
        replay(&account, "FAILED"),
        TradeTransitionResult::Applied(_)
    ));
    let owner = account.instance_snapshot("owner").unwrap();
    close(owner.cash, 100.0);
    close(owner.positions["UP"], 20.0);
    let state = account.lock_state();
    let trade = &state.trades["trade"];
    assert_eq!(
        trade.fee_config.unwrap().settlement,
        FeeSettlement::LegacyV1
    );
    close(trade.fee_config.unwrap().rate, 0.07);
    assert!(validate_persisted_state("v2-fees", &state).is_ok());
}

#[test]
fn zero_fee_attribution_is_frozen_and_new_trade_uses_new_version() {
    let account = seeded(FeeSettlement::LegacyV1, 0.0);
    replay(&account, "CONFIRMED");
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.07,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    replay(&account, "CONFIRMED");
    close(account.instance_snapshot("owner").unwrap().cash, 88.1);
    close(
        account.instance_snapshot("owner").unwrap().positions["UP"],
        34.0,
    );
    account
        .reserve_order(
            "owner",
            "new-order",
            "new-oid",
            "UP",
            Side::Buy,
            14.0,
            0.85,
            700,
        )
        .unwrap();
    assert!(matches!(
        account.apply_trade_transition_with_context(
            "new-trade",
            "CONFIRMED",
            "new-order",
            "new-oid",
            "UP",
            Side::Buy,
            14.0,
            0.85,
            false,
            101
        ),
        TradeTransitionResult::Applied(_)
    ));
    close(
        account.instance_snapshot("owner").unwrap().cash,
        88.1 - 11.9 - 0.12495,
    );
    close(
        account.instance_snapshot("owner").unwrap().positions["UP"],
        48.0,
    );
    assert!(validate_persisted_state("v2-fees", &account.lock_state()).is_ok());
}

#[test]
fn new_pending_trade_binds_v2_but_legacy_unproven_pending_remains_closed() {
    for legacy in [false, true] {
        let account = SharedAccount::new("v2-fees");
        account.register_instance("owner", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        account
            .reserve_order("owner", "order", "oid", "UP", Side::Buy, 14.0, 0.85, 700)
            .unwrap();
        replay(&account, "MATCHED");
        if legacy {
            let mut state = account.lock_state();
            let row = state.trades.get_mut("trade").unwrap();
            row.fee_settlement = Some(FeeSettlement::LegacyV1);
        }
        assert!(account.is_uncertain());
        account
            .register_token_fee_config_with_settlement(
                &["UP".into()],
                0.07,
                1.0,
                FeeSettlement::CollateralV2,
            )
            .unwrap();
        assert_eq!(account.is_uncertain(), legacy);
        let state = account.lock_state();
        assert_eq!(state.fee_attribution_pending.contains("trade"), legacy);
        close(
            state.trades["trade"].usdc_fee,
            if legacy { 0.0 } else { 0.12495 },
        );
        assert!(validate_persisted_state("v2-fees", &state).is_ok());
    }
}

#[test]
fn live_owner_registration_and_failed_replay_keep_original_provenance() {
    let account = Arc::new(seeded(FeeSettlement::LegacyV1, 0.07));
    replay(&account, "MATCHED");
    let lifecycle = account.bind_account_lifecycle_owner().unwrap();
    let owner = std::thread::spawn(move || {
        lifecycle.mark_current_thread().unwrap();
        for _ in 0..2 {
            let command = lifecycle
                .receiver()
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            lifecycle.execute(command);
        }
        let virtual_account = lifecycle.account.virtual_account("owner").unwrap();
        lifecycle
            .account
            .lifecycle(&virtual_account)
            .trades
            .get("trade")
            .unwrap()
            .clone()
    });
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.09,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    assert!(matches!(
        replay(&account, "FAILED"),
        TradeTransitionResult::Applied(_)
    ));
    let row = owner.join().unwrap();
    let virtual_account = account.virtual_account("owner").unwrap();
    close(virtual_account.cash.load(), 100.0);
    close(virtual_account.position("UP").balance.load(), 20.0);
    assert_eq!(row.fee_settlement, Some(FeeSettlement::LegacyV1));
    close(row.fee_config.unwrap().rate, 0.07);
    assert_eq!(account.account_lifecycle_queue_metrics().2, 0);
}

#[test]
fn v2_fee_provenance_survives_wal_restart_and_changed_registry() {
    let path = std::env::temp_dir().join(format!(
        "hexagent-v2-fees-{}-{}.json",
        std::process::id(),
        wall_clock_ms()
    ));
    {
        let account = SharedAccount::new_persistent("v2-fees", &path).unwrap();
        setup(&account, FeeSettlement::CollateralV2, 0.07);
        replay(&account, "MATCHED");
        account.flush_persistence(Duration::from_secs(2)).unwrap();
    }
    {
        let account = SharedAccount::new_persistent("v2-fees", &path).unwrap();
        account
            .register_token_fee_config_with_settlement(
                &["UP".into()],
                0.14,
                1.0,
                FeeSettlement::CollateralV2,
            )
            .unwrap();
        assert!(matches!(
            replay(&account, "FAILED"),
            TradeTransitionResult::Applied(_)
        ));
        close(account.instance_snapshot("owner").unwrap().cash, 100.0);
        close(
            account.instance_snapshot("owner").unwrap().positions["UP"],
            20.0,
        );
        assert!(validate_persisted_state("v2-fees", &account.lock_state()).is_ok());
        account.flush_persistence(Duration::from_secs(2)).unwrap();
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("wal.jsonl"));
}

#[test]
fn explicit_v2_execution_uses_previous_curve_and_preserves_old_trade_replays() {
    let account = seeded(FeeSettlement::LegacyV1, 0.07);
    let update = |status| {
        account.apply_trade_transition_with_context_and_fee_settlement(
            "trade",
            status,
            "order",
            "oid",
            "UP",
            Side::Buy,
            14.0,
            0.85,
            false,
            100,
            FeeSettlement::CollateralV2,
        )
    };
    assert!(matches!(
        update("MATCHED"),
        TradeTransitionResult::Applied(_)
    ));
    assert!(!account.is_uncertain());
    {
        let state = account.lock_state();
        assert_eq!(
            state.trades["trade"].fee_settlement,
            Some(FeeSettlement::CollateralV2)
        );
        close(state.trades["trade"].fee_config.unwrap().rate, 0.07);
        assert!(!state.fee_attribution_pending.contains("trade"));
        assert!(validate_persisted_state("v2-fees", &state).is_ok());
    }
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.09,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    assert!(!account.is_uncertain());
    close(account.instance_snapshot("owner").unwrap().cash, 87.97505);
    close(
        account.instance_snapshot("owner").unwrap().positions["UP"],
        34.0,
    );
    assert!(matches!(
        update("CONFIRMED"),
        TradeTransitionResult::Applied(_)
    ));

    let old = seeded(FeeSettlement::LegacyV1, 0.07);
    replay(&old, "MATCHED");
    old.register_token_fee_config_with_settlement(
        &["UP".into()],
        0.09,
        1.0,
        FeeSettlement::CollateralV2,
    )
    .unwrap();
    // A live V2 adapter carrying an old already-known trade can never rewrite
    // its original V1 fee amount or currency.
    assert!(matches!(
        old.apply_trade_transition_with_context_and_fee_settlement(
            "trade",
            "FAILED",
            "order",
            "oid",
            "UP",
            Side::Buy,
            14.0,
            0.85,
            false,
            100,
            FeeSettlement::CollateralV2,
        ),
        TradeTransitionResult::Applied(_)
    ));
    close(old.instance_snapshot("owner").unwrap().cash, 100.0);
    close(
        old.instance_snapshot("owner").unwrap().positions["UP"],
        20.0,
    );
    let row = old.restored_trades().pop().unwrap();
    assert_eq!(row.fee_settlement, FeeSettlement::LegacyV1);
    close(row.shares_fee, 0.147);
}

#[test]
fn legacy_order_json_keeps_conservative_bps_reservation() {
    let account = seeded(FeeSettlement::LegacyV1, 0.07);
    let mut json = serde_json::to_value(account.order("order").unwrap()).unwrap();
    json.as_object_mut().unwrap().remove("cash_fee_per_share");
    let mut order: OrderOwnership = serde_json::from_value(json).unwrap();
    assert_eq!(order.cash_fee_per_share, None);
    close(order.reservation_cash_per_share(), 0.85 * 1.07);
    order.filled_quantity = 4.0;
    close(desired_order_reservation(&order).0, 10.0 * 0.85 * 1.07);
}

#[test]
fn metadata_switch_cannot_resurrect_redeemed_legacy_shares() {
    let account = seeded(FeeSettlement::LegacyV1, 0.07);
    account
        .register_token_interest("owner", "condition", "UP", "DOWN")
        .unwrap();
    account
        .register_token_interest("sibling", "condition", "UP", "DOWN")
        .unwrap();
    replay(&account, "CONFIRMED");
    account
        .apply_scoped_physical_snapshot(
            188.1,
            HashMap::from([("UP".into(), 53.853)]),
            HashSet::from(["UP".into(), "DOWN".into()]),
        )
        .unwrap();
    account.record_settled_token_values(&HashMap::from([("UP".into(), 1.0), ("DOWN".into(), 0.0)]));
    assert!(account.observe_platform_binary_redeem(
        241.953,
        &HashMap::new(),
        &HashSet::from(["UP".into(), "DOWN".into()])
    ));
    let before = account.instance_snapshot("owner").unwrap();
    close(before.positions.get("UP").copied().unwrap_or(0.0), 0.0);
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.09,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    let after = account.instance_snapshot("owner").unwrap();
    close(after.cash, before.cash);
    close(after.positions.get("UP").copied().unwrap_or(0.0), 0.0);
    replay(&account, "CONFIRMED");
    close(
        account
            .instance_snapshot("owner")
            .unwrap()
            .positions
            .get("UP")
            .copied()
            .unwrap_or(0.0),
        0.0,
    );
    assert!(validate_persisted_state("v2-fees", &account.lock_state()).is_ok());
}

#[test]
fn explicit_fallback_freezes_one_owner_fee_and_does_not_block_new_trades() {
    for maker in [false, true] {
        for prior_rate in [None, Some(0.0), Some(0.06)] {
            let account = SharedAccount::new("v2-fees");
            account.register_instance("owner", 1.0);
            account.apply_physical_snapshot(100.0, HashMap::new()).unwrap();
            if let Some(rate) = prior_rate {
                account.register_token_fee_config(&["UP".into()], rate, 1.0).unwrap();
            }
            account.reserve_order("owner", "order", "oid", "UP", Side::Buy, 14.0, 0.85, 700).unwrap();
            let apply = |status| account.apply_trade_transition_with_context_and_fee_basis(
                "trade", status, "order", "oid", "UP", Side::Buy, 14.0, 0.85,
                maker, 100, FeeBasis { settlement: FeeSettlement::CollateralV2, rate: 0.07, exponent: 1.0 },
            );
            let fee = if maker { 0.0 } else { 14.0 * prior_rate.unwrap_or(0.07) * 0.85 * 0.15 };
            let first = apply("MATCHED");
            let frozen = first.trade_fee().expect("maker and zero-rate also carry explicit proof");
            assert_eq!(frozen.settlement, FeeSettlement::CollateralV2);
            close(frozen.usdc_fee, fee);
            close(frozen.shares_fee, 0.0);
            assert!(!account.is_uncertain(), "missing metadata must not add a gate");
            close(account.instance_snapshot("owner").unwrap().cash, 88.1-fee);
            close(account.instance_snapshot("owner").unwrap().positions["UP"], 14.0);
            account.register_token_fee_config_with_settlement(&["UP".into()], 0.12, 2.0, FeeSettlement::CollateralV2).unwrap();
            assert_eq!(apply("MATCHED").trade_fee(), Some(frozen));
            assert_eq!(apply("MINED").trade_fee(), Some(frozen));
            assert_eq!(apply("FAILED").trade_fee(), Some(frozen));
            assert_eq!(apply("FAILED").trade_fee(), Some(frozen));
            close(account.instance_snapshot("owner").unwrap().cash, 100.0);
            close(account.instance_snapshot("owner").unwrap().positions["UP"], 0.0);
            assert!(validate_persisted_state("v2-fees", &account.lock_state()).is_ok());
        }
    }
}

#[test]
fn explicit_fallback_cannot_upgrade_unproven_historical_legacy_fee() {
    let account = SharedAccount::new("v2-fees");
    account.register_instance("owner", 1.0);
    account.apply_physical_snapshot(100.0, HashMap::new()).unwrap();
    account.reserve_order("owner", "order", "oid", "UP", Side::Buy, 14.0, 0.85, 700).unwrap();
    replay(&account, "MATCHED");
    {
        let mut state = account.lock_state();
        state.trades.get_mut("trade").unwrap().fee_settlement = Some(FeeSettlement::LegacyV1);
    }
    let result = account.apply_trade_transition_with_context_and_fee_basis(
        "trade", "MINED", "order", "oid", "UP", Side::Buy, 14.0, 0.85,
        false, 100, FeeBasis { settlement: FeeSettlement::CollateralV2, rate: 0.07, exponent: 1.0 },
    );
    assert!(result.trade_fee().is_none());
    let state = account.lock_state();
    assert_eq!(state.trades["trade"].fee_settlement, Some(FeeSettlement::LegacyV1));
    assert!(state.trades["trade"].fee_config.is_none());
    assert!(state.fee_attribution_pending.contains("trade"));
}

#[test]
fn explicit_fallback_owner_message_keeps_virtual_fees_frozen() {
    let account = Arc::new(SharedAccount::new("v2-fees"));
    account.register_instance("owner", 1.0);
    account.apply_physical_snapshot(100.0, HashMap::new()).unwrap();
    account.reserve_order("owner", "order", "oid", "UP", Side::Buy, 14.0, 0.85, 700).unwrap();
    let lifecycle = account.bind_account_lifecycle_owner().unwrap();
    let owner = std::thread::spawn(move || {
        lifecycle.mark_current_thread().unwrap();
        for _ in 0..3 {
            let command = lifecycle.receiver().recv_timeout(Duration::from_secs(2)).unwrap();
            lifecycle.execute(command);
        }
    });
    let apply = |status| account.apply_trade_transition_with_context_and_fee_basis(
        "trade", status, "order", "oid", "UP", Side::Buy, 14.0, 0.85,
        false, 100, FeeBasis { settlement: FeeSettlement::CollateralV2, rate: 0.07, exponent: 1.0 },
    );
    let first = apply("MATCHED").trade_fee().unwrap();
    close(first.usdc_fee, 0.12495);
    assert!(!account.is_uncertain());
    account.register_token_fee_config_with_settlement(&["UP".into()], 0.12, 2.0, FeeSettlement::CollateralV2).unwrap();
    assert_eq!(apply("FAILED").trade_fee(), Some(first));
    owner.join().unwrap();
    let virtual_account = account.virtual_account("owner").unwrap();
    close(virtual_account.cash.load(), 100.0);
    close(virtual_account.position("UP").balance.load(), 0.0);
    assert_eq!(account.account_lifecycle_queue_metrics().2, 0);
}

#[test]
fn invalid_explicit_fallback_cannot_create_restart_invalid_trade() {
    let account = seeded(FeeSettlement::CollateralV2, 0.07);
    for (rate, exponent) in [(1.01, 1.0), (0.07, 0.0), (0.07, 10.1), (f64::NAN, 1.0)] {
        assert!(matches!(account.apply_trade_transition_with_context_and_fee_basis(
            "trade", "MATCHED", "order", "oid", "UP", Side::Buy, 14.0, 0.85,
            false, 100, FeeBasis { settlement: FeeSettlement::CollateralV2, rate, exponent },
        ), TradeTransitionResult::Rejected));
        assert!(account.restored_trades().is_empty());
        close(account.instance_snapshot("owner").unwrap().cash, 100.0);
    }
}
