use super::*;

fn seeded(side: Side) -> SharedAccount {
    let account = SharedAccount::new("terminal-fee");
    account.register_instance("owner", 1.0);
    account.register_instance("sibling", 1.0);
    account.apply_physical_snapshot(200.0, HashMap::from([("UP".into(), 40.0)])).unwrap();
    account.register_token_fee_config(&["UP".into()], 0.02, 1.0).unwrap();
    account.reserve_order("owner", "order", "oid", "UP", side, 10.0, 0.5, 0).unwrap();
    account
}

fn replay(account: &SharedAccount, side: Side, status: &str) -> TradeTransitionResult {
    account.apply_trade_transition_with_context(
        "trade", status, "order", "oid", "UP", side, 2.0, 0.5, false, 100,
    )
}

fn trade(account: &SharedAccount) -> AppliedTrade {
    let owner = account.virtual_account("owner").unwrap();
    account.lifecycle(&owner).trades.get("trade").unwrap().clone()
}

fn assert_confirmed_economics(account: &SharedAccount, side: Side) {
    let snapshot = account.instance_snapshot("owner").unwrap();
    let (cash, position, usdc_fee, shares_fee) = match side {
        Side::Buy => (99.0, 21.98, 0.0, 0.02),
        Side::Sell => (100.99, 18.0, 0.01, 0.0),
    };
    assert!((snapshot.cash - cash).abs() < 1e-9, "cash={} expected={cash}", snapshot.cash);
    assert!((snapshot.positions["UP"] - position).abs() < 1e-9,
        "position={} expected={position}", snapshot.positions["UP"]);
    assert_eq!(snapshot.reserved_cash, 0.0);
    assert_eq!(snapshot.reserved_positions.get("UP").copied().unwrap_or(0.0), 0.0);
    let stored = trade(account);
    assert_eq!(stored.ownership.status, "CONFIRMED");
    assert!(stored.booked && stored.physical_booked && !stored.failed);
    assert!(stored.virtual_fee_booked && stored.physical_fee_booked);
    assert!((stored.usdc_fee - usdc_fee).abs() < 1e-9);
    assert!((stored.shares_fee - shares_fee).abs() < 1e-9);
    assert_eq!(account.order("order").unwrap().status, OrderStatus::Cancelled);
    let sibling = account.instance_snapshot("sibling").unwrap();
    assert_eq!(sibling.cash, 100.0);
    assert_eq!(sibling.positions["UP"], 20.0);
}

#[test]
fn confirmed_taker_fee_survives_failed_private_replay_on_both_sides() {
    for side in [Side::Buy, Side::Sell] {
        let account = seeded(side);
        assert!(matches!(replay(&account, side, "MATCHED"), TradeTransitionResult::Applied(_)));
        assert!(!account.mark_cancelled_pending_trade_audit("order", 2.0));
        assert!(matches!(replay(&account, side, "CONFIRMED"), TradeTransitionResult::Applied(_)));
        assert_confirmed_economics(&account, side);
        let generation = trade(&account).ledger_generation;
        // CONFIRMED is terminal for principal. An ignored FAILED must not
        // refund only its fee, and subsequent replays must not toggle it.
        for status in ["FAILED", "FAILED", "MATCHED", "CONFIRMED", "FAILED"] {
            let result = replay(&account, side, status);
            assert!(matches!(result, TradeTransitionResult::OwnedNoop(_)));
            assert_eq!(result.fill_delta(), Some(0.0));
            assert_confirmed_economics(&account, side);
            assert_eq!(trade(&account).ledger_generation, generation);
        }
    }
}

#[test]
fn confirmed_cold_replay_attributes_missing_fee_using_stored_terminal_state() {
    for side in [Side::Buy, Side::Sell] {
        let account = seeded(side);
        // Legacy durable fills may have known principal but unresolved role /
        // fee attribution. A corrected replay supplies that metadata later.
        assert!(account.apply_trade_transition("trade", "CONFIRMED", "order", "oid",
            "UP", side, 2.0, 0.5).is_some());
        assert!(!account.mark_cancelled_pending_trade_audit("order", 2.0));
        assert!(!trade(&account).virtual_fee_booked);
        // An invalid replay enters the normal cold ownership-repair path.
        assert!(matches!(account.apply_trade_transition_with_context(
            "trade", "MATCHED", "order", "oid", "UP", side, 2.0, 0.51, false, 100,
        ), TradeTransitionResult::Rejected));
        assert!(account.anomalous_trade_keys.load().contains("trade"));
        assert!(account.state.lock().unwrap().fee_attribution_pending.contains("trade"));

        let result = replay(&account, side, "FAILED");
        assert!(matches!(result, TradeTransitionResult::OwnedNoop(_)));
        assert_eq!(result.ownership().unwrap().status, "CONFIRMED");
        assert_confirmed_economics(&account, side);
        assert!(!account.anomalous_trade_keys.load().contains("trade"));
        let generation = trade(&account).ledger_generation;
        for status in ["FAILED", "CONFIRMED", "FAILED"] {
            assert!(matches!(replay(&account, side, status), TradeTransitionResult::OwnedNoop(_)));
            assert_confirmed_economics(&account, side);
            assert_eq!(trade(&account).ledger_generation, generation);
        }
    }
}
