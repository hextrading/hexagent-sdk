use super::*;

const ACCOUNT: &str = "execution-price-tests";

fn basis(rate: f64) -> FeeBasis {
    FeeBasis {
        settlement: FeeSettlement::CollateralV2,
        rate,
        exponent: 1.0,
    }
}

fn setup(account: &SharedAccount, side: Side, quantity: f64, limit: f64) {
    account.register_instance("owner", 1.0);
    account.register_instance("sibling", 1.0);
    account
        .apply_physical_snapshot(200.0, HashMap::from([("UP".into(), 40.0)]))
        .unwrap();
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.07,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    account
        .reserve_order("owner", "order", "oid", "UP", side, quantity, limit, 700)
        .unwrap();
}

fn apply(
    account: &SharedAccount,
    status: &str,
    side: Side,
    quantity: f64,
    execution: FrozenTradeExecution,
) -> TradeTransitionResult {
    account.apply_trade_transition_with_frozen_execution(
        "trade", status, "order", "oid", "UP", side, quantity, false, 100, execution,
    )
}

fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "actual={actual:.12} expected={expected:.12}"
    );
}

#[test]
fn twelve_chain_receipts_match_frozen_principal_fee_and_isolated_owner() {
    // Independent OrderFilled/FeeCharged/ERC20/ERC1155 evidence from the
    // current crypto V2 window. This does not assert other-market fee policy.
    for (side, quantity, raw_price, principal, fee) in [
        (Side::Sell, 15.0, 0.16, 2.4327, 0.14267),
        (Side::Buy, 15.0, 0.83, 12.45, 0.14815),
        (Side::Buy, 15.0, 0.85, 12.75, 0.13387),
        (Side::Buy, 15.0, 0.84, 12.6, 0.14112),
        (Side::Buy, 13.0, 0.95, 12.35, 0.04322),
        (Side::Buy, 12.0, 0.24, 2.88, 0.15321),
        (Side::Buy, 15.0, 0.96, 14.4, 0.04032),
        (Side::Sell, 13.0, 0.12, 1.56, 0.09609),
        (Side::Sell, 12.0, 0.78, 9.36, 0.14414),
        (Side::Sell, 7.0, 0.59, 4.13, 0.11853),
        (Side::Sell, 14.0, 0.02, 0.28, 0.01920),
        (Side::Sell, 14.0, 0.99, 13.86, 0.00970),
    ] {
        let account = SharedAccount::new(ACCOUNT);
        setup(&account, side, quantity, raw_price);
        let frozen =
            FrozenTradeExecution::new(raw_price, quantity, principal, side, false, basis(0.07))
                .unwrap();
        close(frozen.price, principal / quantity);
        close(frozen.fee.usdc_fee, fee);
        close(frozen.fee.shares_fee, 0.0);
        let direction = if side == Side::Buy { -1.0 } else { 1.0 };
        for status in [
            "MATCHED",
            "MATCHED",
            "MINED",
            "CONFIRMED",
            "MATCHED",
            "CONFIRMED",
        ] {
            let result = apply(&account, status, side, quantity, frozen);
            assert!(
                !matches!(result, TradeTransitionResult::Rejected),
                "{side:?} {raw_price} {status}"
            );
            close(result.ownership().unwrap().price, principal / quantity);
            assert_eq!(result.trade_fee(), Some(frozen.fee));
            let owner = account.instance_snapshot("owner").unwrap();
            close(owner.cash, 100.0 + direction * principal - fee);
            close(owner.positions["UP"], 20.0 - direction * quantity);
            let sibling = account.instance_snapshot("sibling").unwrap();
            close(sibling.cash, 100.0);
            close(sibling.positions["UP"], 20.0);
            assert!(validate_persisted_state(ACCOUNT, &account.lock_state()).is_ok());
        }
    }
}

#[test]
fn metadata_change_between_fast_selection_and_cold_apply_cannot_change_message_fee() {
    let account = SharedAccount::new(ACCOUNT);
    setup(&account, Side::Sell, 15.0, 0.16);
    let frozen =
        FrozenTradeExecution::new(0.16, 15.0, 2.4327, Side::Sell, false, basis(0.07)).unwrap();
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.08,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    let first = apply(&account, "MATCHED", Side::Sell, 15.0, frozen);
    assert_eq!(first.trade_fee(), Some(frozen.fee));
    close(account.instance_snapshot("owner").unwrap().cash, 102.29003);
    for status in ["FAILED", "FAILED", "MATCHED", "CONFIRMED"] {
        let result = apply(&account, status, Side::Sell, 15.0, frozen);
        assert!(!matches!(result, TradeTransitionResult::Rejected));
        assert_eq!(result.trade_fee(), Some(frozen.fee));
        close(account.instance_snapshot("owner").unwrap().cash, 100.0);
        close(
            account.instance_snapshot("owner").unwrap().positions["UP"],
            20.0,
        );
    }
    let next =
        FrozenTradeExecution::new(0.16, 15.0, 2.4327, Side::Sell, false, basis(0.08)).unwrap();
    assert!(next.fee.usdc_fee > frozen.fee.usdc_fee);
    assert!(validate_persisted_state(ACCOUNT, &account.lock_state()).is_ok());
}

#[test]
fn old_top_price_v2_row_keeps_original_economics_when_replayed_with_new_legs() {
    let account = SharedAccount::new(ACCOUNT);
    setup(&account, Side::Sell, 15.0, 0.16);
    let old = account.apply_trade_transition_with_context_and_fee_basis(
        "trade",
        "MATCHED",
        "order",
        "oid",
        "UP",
        Side::Sell,
        15.0,
        0.16,
        false,
        100,
        basis(0.07),
    );
    close(old.trade_fee().unwrap().usdc_fee, 0.14112);
    {
        let mut state = account.lock_state();
        let mut serialized = serde_json::to_value(&state.trades["trade"]).unwrap();
        serialized
            .as_object_mut()
            .unwrap()
            .remove("execution_pricing");
        let restored: AppliedTrade = serde_json::from_value(serialized).unwrap();
        assert!(restored.execution_pricing.is_none());
        state.trades.insert("trade".into(), restored);
    }
    for status in ["MINED", "FAILED", "FAILED"] {
        let result = account.apply_trade_transition_with_context_and_execution(
            "trade",
            status,
            "order",
            "oid",
            "UP",
            Side::Sell,
            15.0,
            0.16,
            false,
            100,
            basis(0.07),
            2.4327,
        );
        assert!(!matches!(result, TradeTransitionResult::Rejected));
        close(result.ownership().unwrap().price, 0.16);
        close(result.trade_fee().unwrap().usdc_fee, 0.14112);
        close(
            account.instance_snapshot("owner").unwrap().cash,
            if status == "MINED" { 102.25888 } else { 100.0 },
        );
        assert!(account.lock_state().trades["trade"]
            .execution_pricing
            .is_none());
    }
    assert!(validate_persisted_state(ACCOUNT, &account.lock_state()).is_ok());
}

#[test]
fn actual_vwap_and_rounded_fee_survive_wal_restart_then_reverse_once() {
    let path = std::env::temp_dir().join(format!(
        "hexagent-execution-price-{}-{}.json",
        std::process::id(),
        wall_clock_ms()
    ));
    let frozen =
        FrozenTradeExecution::new(0.16, 15.0, 2.4327, Side::Sell, false, basis(0.07)).unwrap();
    {
        let account = SharedAccount::new_persistent(ACCOUNT, &path).unwrap();
        setup(&account, Side::Sell, 15.0, 0.16);
        assert!(!matches!(
            apply(&account, "MATCHED", Side::Sell, 15.0, frozen),
            TradeTransitionResult::Rejected
        ));
        account.flush_persistence(Duration::from_secs(2)).unwrap();
    }
    {
        let account = SharedAccount::new_persistent(ACCOUNT, &path).unwrap();
        account
            .register_token_fee_config_with_settlement(
                &["UP".into()],
                0.09,
                1.0,
                FeeSettlement::CollateralV2,
            )
            .unwrap();
        for status in ["MINED", "FAILED", "FAILED"] {
            let result = apply(&account, status, Side::Sell, 15.0, frozen);
            assert!(!matches!(result, TradeTransitionResult::Rejected));
            close(result.ownership().unwrap().price, 0.16218);
            close(result.trade_fee().unwrap().usdc_fee, 0.14267);
            close(
                account.instance_snapshot("owner").unwrap().cash,
                if status == "MINED" { 102.29003 } else { 100.0 },
            );
        }
        assert!(validate_persisted_state(ACCOUNT, &account.lock_state()).is_ok());
        account.flush_persistence(Duration::from_secs(2)).unwrap();
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(persistence_wal_path(&path));
}

#[test]
fn restored_one_ulp_execution_replay_preserves_frozen_economics_and_clears_false_anomaly() {
    // maker02: all three confirmed taker sells had gross=19.6, quantity=20,
    // persisted price=0.98 while gross/quantity is the adjacent f64.
    let path = std::env::temp_dir().join(format!(
        "hexagent-execution-ulp-{}-{}.json",
        std::process::id(),
        wall_clock_ms()
    ));
    let original =
        FrozenTradeExecution::new(0.98, 20.0, 19.6, Side::Sell, false, basis(0.07)).unwrap();
    {
        let account = SharedAccount::new_persistent(ACCOUNT, &path).unwrap();
        setup(&account, Side::Sell, 20.0, 0.98);
        for status in ["MATCHED", "CONFIRMED"] {
            assert!(!matches!(
                apply(&account, status, Side::Sell, 20.0, original),
                TradeTransitionResult::Rejected
            ));
        }
        account.flush_persistence(Duration::from_secs(2)).unwrap();
    }
    {
        let account = SharedAccount::new_persistent(ACCOUNT, &path).unwrap();
        let restored = account
            .private_execution_seed_for_trade("trade")
            .unwrap()
            .execution
            .unwrap();
        assert_eq!(restored.price, 0.98);
        assert_eq!(
            restored
                .price
                .to_bits()
                .abs_diff((19.6_f64 / 20.0).to_bits()),
            1
        );
        assert_eq!(restored.fee.usdc_fee, 0.02744);
        // Simulate the exact prior fallback symptom; matching replay must
        // recover it through the normal durable validator, never a manual clear.
        assert!(matches!(
            account.record_authenticated_terminal_trade_noop(
                "trade",
                "CONFIRMED",
                "oid",
                "UP",
                Side::Sell,
                20.0,
                0.98,
                false,
            ),
            TradeTransitionResult::Rejected
        ));
        let owner_before = account.instance_snapshot("owner").unwrap();
        let sibling_before = account.instance_snapshot("sibling").unwrap();
        let row_before = serde_json::to_value(&account.lock_state().trades["trade"]).unwrap();
        for status in ["CONFIRMED", "CONFIRMED", "MATCHED"] {
            let result = apply(&account, status, Side::Sell, 20.0, restored);
            assert!(
                matches!(
                    result,
                    TradeTransitionResult::OwnedNoop(_)
                        | TradeTransitionResult::OwnedNoopButPersistencePending(_)
                ),
                "{result:?}"
            );
            assert_eq!(result.fill_delta(), Some(0.0));
            assert_eq!(result.trade_fee(), Some(restored.fee));
            assert_eq!(
                serde_json::to_value(&account.lock_state().trades["trade"]).unwrap(),
                row_before
            );
            assert_eq!(
                account.instance_snapshot("owner").unwrap().cash,
                owner_before.cash
            );
            assert_eq!(
                account.instance_snapshot("sibling").unwrap().cash,
                sibling_before.cash
            );
        }
        assert!(!account
            .lock_state()
            .ownership_anomalies
            .contains_key("trade:trade"));
        assert!(validate_persisted_state(ACCOUNT, &account.lock_state()).is_ok());
        account.flush_persistence(Duration::from_secs(2)).unwrap();
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(persistence_wal_path(&path));
}

#[test]
fn restored_execution_exception_cannot_admit_new_or_changed_economics_or_foreign_identity() {
    let account = SharedAccount::new(ACCOUNT);
    setup(&account, Side::Sell, 20.0, 0.98);
    let original =
        FrozenTradeExecution::new(0.98, 20.0, 19.6, Side::Sell, false, basis(0.07)).unwrap();
    let restored = FrozenTradeExecution {
        price: 0.98,
        ..original
    };
    assert!(matches!(
        apply(&account, "MATCHED", Side::Sell, 20.0, restored),
        TradeTransitionResult::Rejected
    ));
    assert!(!matches!(
        apply(&account, "MATCHED", Side::Sell, 20.0, original),
        TradeTransitionResult::Rejected
    ));
    // Same serialization boundary as a retained row restored from JSON.
    {
        let mut state = account.lock_state();
        let raw = serde_json::to_string(&state.trades["trade"]).unwrap();
        state
            .trades
            .insert("trade".into(), serde_json::from_str(&raw).unwrap());
    }
    let restored = account
        .private_execution_seed_for_trade("trade")
        .unwrap()
        .execution
        .unwrap();
    assert_eq!(restored.price, 0.98);
    let before = serde_json::to_value(&*account.lock_state()).unwrap();
    let mut changed_fee = restored;
    changed_fee.fee.usdc_fee = f64::from_bits(changed_fee.fee.usdc_fee.to_bits() + 1);
    for dto in [
        changed_fee,
        FrozenTradeExecution {
            price: 0.97999,
            ..restored
        },
        FrozenTradeExecution {
            gross_notional: Some(19.600001),
            ..restored
        },
        FrozenTradeExecution {
            fee_basis: basis(0.08),
            ..restored
        },
    ] {
        assert!(matches!(
            apply(&account, "MINED", Side::Sell, 20.0, dto),
            TradeTransitionResult::Rejected
        ));
        assert_eq!(
            serde_json::to_value(&*account.lock_state()).unwrap(),
            before
        );
    }
    for (trade, coid, oid, token, side, quantity, maker) in [
        ("different", "order", "oid", "UP", Side::Sell, 20.0, false),
        (
            "trade",
            "sibling-order",
            "oid",
            "UP",
            Side::Sell,
            20.0,
            false,
        ),
        (
            "trade",
            "order",
            "foreign-oid",
            "UP",
            Side::Sell,
            20.0,
            false,
        ),
        ("trade", "order", "oid", "FOREIGN", Side::Sell, 20.0, false),
        ("trade", "order", "oid", "UP", Side::Buy, 20.0, false),
        ("trade", "order", "oid", "UP", Side::Sell, 20.01, false),
        ("trade", "order", "oid", "UP", Side::Sell, 20.0, true),
    ] {
        assert!(matches!(
            account.apply_trade_transition_with_frozen_execution(
                trade, "MINED", coid, oid, token, side, quantity, maker, 100, restored,
            ),
            TradeTransitionResult::Rejected
        ));
        assert_eq!(
            serde_json::to_value(&*account.lock_state()).unwrap(),
            before
        );
    }
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            0.09,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    for status in ["MINED", "FAILED", "FAILED"] {
        let result = apply(&account, status, Side::Sell, 20.0, restored);
        assert!(!matches!(result, TradeTransitionResult::Rejected));
        assert_eq!(result.trade_fee(), Some(restored.fee));
        close(
            account.instance_snapshot("owner").unwrap().cash,
            if status == "MINED" { 119.57256 } else { 100.0 },
        );
        close(account.instance_snapshot("sibling").unwrap().cash, 100.0);
    }
}

#[test]
fn conflicting_or_non_finite_execution_cannot_mutate_owner_economics() {
    let account = SharedAccount::new(ACCOUNT);
    setup(&account, Side::Sell, 15.0, 0.16);
    let original =
        FrozenTradeExecution::new(0.16, 15.0, 2.4327, Side::Sell, false, basis(0.07)).unwrap();
    assert!(!matches!(
        apply(&account, "MATCHED", Side::Sell, 15.0, original),
        TradeTransitionResult::Rejected
    ));
    for gross in [2.45, f64::NAN, f64::INFINITY, -1.0] {
        let mut invalid = original;
        invalid.gross_notional = Some(gross);
        assert!(matches!(
            apply(&account, "MINED", Side::Sell, 15.0, invalid),
            TradeTransitionResult::Rejected
        ));
        close(account.instance_snapshot("owner").unwrap().cash, 102.29003);
        close(
            account.instance_snapshot("owner").unwrap().positions["UP"],
            5.0,
        );
    }
}

#[test]
fn public_execution_dto_rejects_invalid_numbers_before_any_ledger_mutation() {
    let account = SharedAccount::new(ACCOUNT);
    setup(&account, Side::Sell, 15.0, 0.16);
    let valid =
        FrozenTradeExecution::new(0.16, 15.0, 2.4327, Side::Sell, false, basis(0.07)).unwrap();
    let before = serde_json::to_value(&*account.lock_state()).unwrap();
    let mut invalid = Vec::new();
    for value in [0.0, 1.0, -1.0, f64::NAN, f64::INFINITY] {
        invalid.push(FrozenTradeExecution {
            raw_price: value,
            ..valid
        });
        invalid.push(FrozenTradeExecution {
            price: value,
            ..valid
        });
    }
    for gross in [0.0, 15.0, 15.00001, -1.0, f64::NAN, f64::INFINITY] {
        invalid.push(FrozenTradeExecution {
            gross_notional: Some(gross),
            ..valid
        });
    }
    // None is a historical top-price row, never permission to send a different
    // canonical price without durable principal provenance.
    invalid.push(FrozenTradeExecution {
        gross_notional: None,
        ..valid
    });
    for amount in [-1e-12, f64::NAN, f64::INFINITY] {
        let mut cash = valid;
        cash.fee.usdc_fee = amount;
        invalid.push(cash);
        let mut shares = valid;
        shares.fee.shares_fee = amount;
        invalid.push(shares);
    }
    for rate in [-0.01, 1.01, f64::NAN, f64::INFINITY] {
        invalid.push(FrozenTradeExecution {
            fee_basis: basis(rate),
            ..valid
        });
    }
    let mut wrong_currency = valid;
    wrong_currency.fee.settlement = FeeSettlement::LegacyV1;
    invalid.push(wrong_currency);
    for execution in invalid {
        assert!(
            matches!(
                apply(&account, "MATCHED", Side::Sell, 15.0, execution),
                TradeTransitionResult::Rejected
            ),
            "accepted invalid DTO: {execution:?}"
        );
        assert_eq!(
            serde_json::to_value(&*account.lock_state()).unwrap(),
            before
        );
    }
    for quantity in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert!(matches!(
            apply(&account, "MATCHED", Side::Sell, quantity, valid),
            TradeTransitionResult::Rejected
        ));
        assert_eq!(
            serde_json::to_value(&*account.lock_state()).unwrap(),
            before
        );
    }
    assert!(!matches!(
        apply(&account, "MATCHED", Side::Sell, 15.0, valid),
        TradeTransitionResult::Rejected
    ));
}

#[test]
fn execution_constructor_excludes_settlement_prices_and_non_finite_inputs() {
    for (raw, quantity, gross) in [
        (0.16, 15.0, 15.0),
        (0.16, 15.0, 15.00001),
        (1.0, 15.0, 2.4),
        (0.0, 15.0, 2.4),
        (f64::NAN, 15.0, 2.4),
        (0.16, f64::INFINITY, 2.4),
        (0.16, 0.0, 2.4),
        (0.16, 15.0, f64::NAN),
        (0.16, 15.0, f64::INFINITY),
        (0.5, f64::MAX, f64::MAX * 0.5),
    ] {
        assert!(
            FrozenTradeExecution::new(raw, quantity, gross, Side::Sell, false, basis(0.07))
                .is_err()
        );
    }
}

#[test]
fn persisted_terminal_tombstone_validates_actual_principal_provenance() {
    let account = SharedAccount::new(ACCOUNT);
    account.register_instance("owner", 1.0);
    let pricing = TradeExecutionPricing {
        raw_price: 0.16,
        gross_notional: 2.4327,
    };
    let mut state = account.lock_state();
    state.retired_trade_ownership_tombstones.insert(
        "trade".into(),
        RetiredTradeOwnershipTombstone {
            ownership: TradeOwnership {
                order_slot: Default::default(),
                account_id: ACCOUNT.into(),
                instance_id: "owner".into(),
                trade_key: "trade".into(),
                client_order_id: "order".into(),
                order_id: "oid".into(),
                token_id: "UP".into(),
                side: Side::Sell,
                quantity: 15.0,
                price: 2.4327 / 15.0,
                status: "CONFIRMED".into(),
            },
            execution_pricing: Some(pricing),
            is_maker: Some(false),
            authenticated_terminal_noop: false,
            retired_at_ms: 100,
        },
    );
    assert!(validate_persisted_state(ACCOUNT, &state).is_ok());
    for malformed in [
        TradeExecutionPricing {
            raw_price: 1.0,
            ..pricing
        },
        TradeExecutionPricing {
            raw_price: f64::NAN,
            ..pricing
        },
        TradeExecutionPricing {
            gross_notional: 15.0,
            ..pricing
        },
        TradeExecutionPricing {
            gross_notional: -1.0,
            ..pricing
        },
        TradeExecutionPricing {
            gross_notional: f64::INFINITY,
            ..pricing
        },
        TradeExecutionPricing {
            gross_notional: 2.4,
            ..pricing
        },
    ] {
        state
            .retired_trade_ownership_tombstones
            .get_mut("trade")
            .unwrap()
            .execution_pricing = Some(malformed);
        let error = validate_persisted_state(ACCOUNT, &state).unwrap_err();
        assert!(error.contains("retired trade tombstone `trade`"), "{error}");
    }
    // Missing provenance is a supported old schema, not a migration trigger.
    state
        .retired_trade_ownership_tombstones
        .get_mut("trade")
        .unwrap()
        .execution_pricing = None;
    assert!(validate_persisted_state(ACCOUNT, &state).is_ok());
}

#[test]
fn startup_seed_rejects_unknown_role_but_retains_known_unattributed_history() {
    let account = SharedAccount::new(ACCOUNT);
    setup(&account, Side::Sell, 15.0, 0.16);
    let frozen =
        FrozenTradeExecution::new(0.16, 15.0, 2.4327, Side::Sell, false, basis(0.07)).unwrap();
    assert!(!matches!(
        apply(&account, "MATCHED", Side::Sell, 15.0, frozen),
        TradeTransitionResult::Rejected
    ));
    account
        .lock_state()
        .trades
        .get_mut("trade")
        .unwrap()
        .is_maker = None;
    assert!(account.private_execution_seed_checked().is_err());
    {
        let mut state = account.lock_state();
        let row = state.trades.get_mut("trade").unwrap();
        row.is_maker = Some(false);
        row.virtual_fee_booked = false;
        row.physical_fee_booked = false;
        row.usdc_fee = 0.0;
        row.shares_fee = 0.0;
        row.fee_config = None;
        state.fee_attribution_pending.insert("trade".into());
    }
    let seeds = account.private_execution_seed_checked().unwrap();
    let retained = seeds
        .iter()
        .find(|row| row.ownership.trade_key == "trade")
        .unwrap();
    assert!(!retained.is_maker);
    assert!(retained.execution.is_none());
}

#[test]
fn load_compatible_historical_fee_replays_and_reverses_exact_stored_amount() {
    let account = SharedAccount::new(ACCOUNT);
    setup(&account, Side::Sell, 15.0, 0.16);
    let stored_fee = 0.14112 + 5e-7;
    let old_basis = basis(stored_fee / (15.0 * 0.16 * 0.84));
    account
        .register_token_fee_config_with_settlement(
            &["UP".into()],
            old_basis.rate,
            old_basis.exponent,
            old_basis.settlement,
        )
        .unwrap();
    let first = account.apply_trade_transition_with_context_and_fee_basis(
        "trade",
        "MATCHED",
        "order",
        "oid",
        "UP",
        Side::Sell,
        15.0,
        0.16,
        false,
        100,
        old_basis,
    );
    assert!(!matches!(first, TradeTransitionResult::Rejected));
    close(first.trade_fee().unwrap().usdc_fee, stored_fee);
    {
        // A historical schema may omit the per-row curve while the retained
        // registry differs within the existing durable loader's tolerance.
        // Preserve the actual booked cash and stored fee, not a new estimate.
        let mut state = account.lock_state();
        state.trades.get_mut("trade").unwrap().fee_config = None;
        state.token_fee_configs.insert("UP".into(), basis(0.07));
        assert!(validate_persisted_state(ACCOUNT, &state).is_ok());
    }
    let seeds = account.private_execution_seed_checked().unwrap();
    let frozen = seeds
        .iter()
        .find(|row| row.ownership.trade_key == "trade")
        .unwrap()
        .execution
        .unwrap();
    assert!(frozen.gross_notional.is_none());
    close(frozen.fee.usdc_fee, stored_fee);
    for status in ["MINED", "FAILED", "FAILED"] {
        let result = apply(&account, status, Side::Sell, 15.0, frozen);
        assert!(
            !matches!(result, TradeTransitionResult::Rejected),
            "{status}"
        );
        assert_eq!(result.ownership().unwrap().status, status);
        close(result.trade_fee().unwrap().usdc_fee, stored_fee);
        close(
            account.instance_snapshot("owner").unwrap().cash,
            if status == "MINED" {
                102.4 - stored_fee
            } else {
                100.0
            },
        );
        let state = account.lock_state();
        assert_eq!(state.trades["trade"].ownership.status, status);
        assert!(state.trades["trade"].execution_pricing.is_none());
        assert!(validate_persisted_state(ACCOUNT, &state).is_ok());
    }
}
