use super::*;

fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "actual={actual} expected={expected}"
    );
}

#[test]
fn v2_buy_cash_fee_matches_scan_and_incremental_through_failed_and_restart() {
    for incremental in [false, true] {
        let mut pm = PositionManager::with_initial_quantities(HashMap::new(), 100.0);
        if incremental {
            pm.enable_incremental_queries();
        }
        let apply = |pm: &mut PositionManager, status| {
            pm.upsert_trade_with_fee_settlement(
                "trade",
                "UP",
                Side::Buy,
                14.0,
                0.85,
                status,
                false,
                0.12495,
                0.0,
                None,
                FeeSettlement::CollateralV2,
            )
        };
        assert_eq!(apply(&mut pm, TradeStatus::Matched).accumulator_sign, 1);
        close(pm.balance(), 87.97505);
        close(pm.available_cash(), 87.97505);
        close(pm.get_quantity("UP"), 14.0);
        assert!(!apply(&mut pm, TradeStatus::Matched).applied);
        let json = serde_json::to_vec(&pm.snapshot()).unwrap();
        let mut restored =
            PositionManager::from_snapshot(serde_json::from_slice(&json).unwrap()).unwrap();
        if incremental {
            restored.enable_incremental_queries();
        }
        assert_eq!(
            restored.trades()["trade"].fee_settlement,
            FeeSettlement::CollateralV2
        );
        assert_eq!(
            apply(&mut restored, TradeStatus::Failed).accumulator_sign,
            -1
        );
        close(restored.balance(), 100.0);
        close(restored.available_cash(), 100.0);
        close(restored.get_quantity("UP"), 0.0);
        assert!(!apply(&mut restored, TradeStatus::Confirmed).applied);
        close(restored.balance(), 100.0);
    }
}

#[test]
fn fee_versioned_pending_budget_partial_failed_and_snapshot_validation() {
    let mut pm = PositionManager::with_initial_quantities(HashMap::new(), 100.0);
    pm.register_pending_order_with_cash_fee("order", "UP", Side::Buy, 0.85, 14.0, 0.85 * 0.07);
    close(pm.locked_buy_cost(), 12.733);
    assert!(pm.apply_private_trade_reservation("order", 4.0, 1));
    close(pm.locked_buy_cost(), 10.0 * 0.85 * 1.07);
    let snapshot = pm.snapshot();
    let mut restored = PositionManager::from_snapshot(snapshot.clone()).unwrap();
    assert!(restored.apply_private_trade_reservation("order", 4.0, -1));
    close(restored.locked_buy_cost(), 12.733);
    restored.remove_pending_order("order");
    close(restored.locked_buy_cost(), 0.0);
    for fee in [f64::NAN, f64::INFINITY, -0.1] {
        let mut invalid = snapshot.clone();
        invalid
            .pending_orders
            .get_mut("order")
            .unwrap()
            .cash_fee_per_share = fee;
        assert!(PositionManager::from_snapshot(invalid).is_err());
    }
}

#[test]
fn historical_pm_json_without_currency_or_pending_fee_retains_v1() {
    let mut pm = PositionManager::with_initial_quantities(HashMap::new(), 100.0);
    pm.upsert_trade(
        "legacy",
        "UP",
        Side::Buy,
        14.0,
        0.85,
        TradeStatus::Matched,
        false,
        0.0,
        0.147,
        None,
    );
    pm.register_pending_order("order", "UP", Side::Buy, 0.85, 2.0);
    let mut json = serde_json::to_value(pm.snapshot()).unwrap();
    json["trades"]["legacy"]
        .as_object_mut()
        .unwrap()
        .remove("fee_settlement");
    json["trades"]["legacy"]
        .as_object_mut()
        .unwrap()
        .remove("fee_attributed");
    json["pending_orders"]["order"]
        .as_object_mut()
        .unwrap()
        .remove("cash_fee_per_share");
    let mut restored =
        PositionManager::from_snapshot(serde_json::from_value(json).unwrap()).unwrap();
    close(restored.balance(), 88.1);
    close(restored.get_quantity("UP"), 13.853);
    close(restored.locked_buy_cost(), 1.7);
    assert_eq!(
        restored.trades()["legacy"].fee_settlement,
        FeeSettlement::LegacyV1
    );
    assert!(
        !restored
            .upsert_trade_with_fee_settlement(
                "legacy",
                "UP",
                Side::Buy,
                14.0,
                0.85,
                TradeStatus::Failed,
                false,
                0.12495,
                0.0,
                None,
                FeeSettlement::CollateralV2
            )
            .applied
    );
    assert_eq!(
        restored
            .upsert_trade(
                "legacy",
                "UP",
                Side::Buy,
                14.0,
                0.85,
                TradeStatus::Failed,
                false,
                0.0,
                0.147,
                None
            )
            .accumulator_sign,
        -1
    );
    close(restored.balance(), 100.0);
}

#[test]
fn snapshot_rejects_cross_currency_even_when_cash_and_shares_are_finite() {
    let mut pm = PositionManager::with_initial_quantities(HashMap::new(), 100.0);
    pm.upsert_trade_with_fee_settlement(
        "v2",
        "UP",
        Side::Buy,
        14.0,
        0.85,
        TradeStatus::Confirmed,
        false,
        0.12495,
        0.0,
        None,
        FeeSettlement::CollateralV2,
    );
    let mut snapshot = pm.snapshot();
    snapshot.trades.get_mut("v2").unwrap().shares_fee = 0.147;
    assert!(PositionManager::from_snapshot(snapshot).is_err());
}

fn restored_fee_row(status: &str, attributed: bool) -> RestoredTrade {
    RestoredTrade {
        ownership: crate::account::shared_account::TradeOwnership {
            order_slot: Default::default(),
            account_id: "wallet".into(),
            instance_id: "owner".into(),
            trade_key: "trade".into(),
            client_order_id: "order".into(),
            order_id: "oid".into(),
            token_id: "UP".into(),
            side: Side::Buy,
            quantity: 14.0,
            price: 0.85,
            status: status.into(),
        },
        booked: status != "FAILED",
        usdc_fee: if attributed { 0.12495 } else { 0.0 },
        shares_fee: 0.0,
        virtual_fee_booked: attributed && status != "FAILED",
        is_maker: false,
        match_time_secs: 100,
        ledger_generation: 1,
        fee_settlement: FeeSettlement::CollateralV2,
        fee_attributed: attributed,
    }
}

#[test]
fn restored_gross_trade_binds_fee_once_without_replaying_principal() {
    for incremental in [false, true] {
        for status in ["MATCHED", "CONFIRMED", "FAILED"] {
            let pending = restored_fee_row(status, false);
            let live = status != "FAILED";
            let mut pm = PositionManager::with_positions_and_restored_trades(
                HashMap::from([(
                    "UP".into(),
                    Position {
                        quantity: if live { 14.0 } else { 0.0 },
                        avg_price: 0.85,
                        current_value: 0.0,
                    },
                )]),
                if live { 88.1 } else { 100.0 },
                [pending],
            );
            if incremental {
                pm.enable_incremental_queries();
            }
            assert!(!pm.trades()["trade"].fee_attributed);
            let attributed = restored_fee_row(status, true);
            let delta = pm.attribute_restored_trade_fee(&attributed).unwrap();
            assert!(delta.applied);
            close(delta.cash_delta, if live { -0.12495 } else { 0.0 });
            close(delta.quantity_delta, 0.0);
            close(pm.balance(), if live { 87.97505 } else { 100.0 });
            close(pm.get_quantity("UP"), if live { 14.0 } else { 0.0 });
            assert!(
                !pm.attribute_restored_trade_fee(&attributed)
                    .unwrap()
                    .applied
            );
            let mut repriced = attributed.clone();
            repriced.usdc_fee = 0.25;
            assert!(pm.attribute_restored_trade_fee(&repriced).is_err());
            let encoded = serde_json::to_vec(&pm.snapshot()).unwrap();
            let restored =
                PositionManager::from_snapshot(serde_json::from_slice(&encoded).unwrap()).unwrap();
            assert!(restored.trades()["trade"].fee_attributed);
            close(restored.balance(), pm.balance());
        }
    }
}

#[test]
fn deferred_fee_then_failed_is_one_net_gross_reversal() {
    let mut pm = PositionManager::with_positions_and_restored_trades(
        HashMap::from([(
            "UP".into(),
            Position {
                quantity: 14.0,
                avg_price: 0.85,
                current_value: 0.0,
            },
        )]),
        88.1,
        [restored_fee_row("MATCHED", false)],
    );
    pm.enable_incremental_queries();
    let failed = restored_fee_row("FAILED", true);
    let delta = pm.attribute_restored_trade_fee(&failed).unwrap();
    close(delta.cash_delta, -0.12495);
    let result = pm.upsert_trade_with_fee_settlement(
        "trade",
        "UP",
        Side::Buy,
        14.0,
        0.85,
        TradeStatus::Failed,
        false,
        failed.usdc_fee,
        failed.shares_fee,
        None,
        failed.fee_settlement,
    );
    assert_eq!(result.accumulator_sign, -1);
    close(pm.balance(), 100.0);
    close(pm.available_cash(), 100.0);
    close(pm.get_quantity("UP"), 0.0);
    assert!(!pm.attribute_restored_trade_fee(&failed).unwrap().applied);
}
