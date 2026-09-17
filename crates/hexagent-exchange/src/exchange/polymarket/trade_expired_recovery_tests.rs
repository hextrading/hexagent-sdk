use super::reconcile_identity_tests::{install, ownership, tracked};
use super::*;

fn absent(orders: &[OrderOwnership]) -> RuntimeOrderAuditPass {
    RuntimeOrderAuditPass {
        updates: vec![],
        errors: orders
            .iter()
            .map(|o| format!("{}: JSON null", o.client_order_id))
            .collect(),
        not_found: vec![],
        retired_market_absent: orders
            .iter()
            .map(|o| RuntimeMissingOrder {
                client_order_id: o.client_order_id.clone(),
                tracked: tracked(o),
                order_id: o.order_id.clone(),
                evidence: "single_order_lookup_json_null".into(),
            })
            .collect(),
    }
}

fn no_fill() -> HistoricalOrderTradeAudit {
    HistoricalOrderTradeAudit::CompleteNoFill {
        pages: 1,
        after_secs: 1_789_580_000,
    }
}

#[test]
fn runtime_history_keeps_market_filter_and_exact_order_identity_on_every_page() {
    let market = format!("0x{:064x}", 42);
    let mut pages = 0;
    let result = fetch_historical_order_trade_audit_in_market(
        "0xTarget",
        1_789_584_642_112,
        Some(&market),
        |path| {
            let url = reqwest::Url::parse(&format!("https://example.invalid{path}")).unwrap();
            let query: HashMap<_, _> = url.query_pairs().into_owned().collect();
            assert_eq!(query.get("market"), Some(&market));
            assert_eq!(query.get("after").map(String::as_str), Some("1789584342"));
            pages += 1;
            Ok(if pages == 1 {
                serde_json::json!({"data":[],"next_cursor":"cursor=+&"})
            } else {
                assert_eq!(
                    query.get("next_cursor").map(String::as_str),
                    Some("cursor=+&")
                );
                serde_json::json!({"data":[{"id":"trade-id","taker_order_id":"0xother","maker_orders":[{"order_id":"0xtarget"}]}],"next_cursor":"LTE="})
            })
        },
    );
    assert_eq!(pages, 2);
    assert!(matches!(
        result,
        HistoricalOrderTradeAudit::FoundFill { records: 1 }
    ));
}

fn finish(trade: PolymarketTrade, shutdown: ShutdownToken) {
    shutdown.request();
    shutdown.finish();
    trade.shared.join_background_workers();
}

#[test]
fn expired_runtime_null_orders_recover_on_first_complete_history_pass() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    let sibling = ownership(Side::Buy, "sibling-1789584630000", "0xsibling", "sibling");
    install(&shared, &sibling);
    for (side, coid, oid) in [
        (Side::Sell, "btc01-1789584642112", "0xfirst"),
        (Side::Buy, "btc01-1789584642114", "0xsecond"),
    ] {
        let mut order = ownership(side, coid, oid, "btc01");
        order.status = OrderStatus::NewOrderTimeout;
        install(&shared, &order);
        let pass = absent(&[order.clone()]);
        // This was the incident's dead end: one JSON-null lookup could not
        // enter the parallel fast path, and tokens were not yet retired.
        assert!(!retired_market_parallel_absence_fast_path_allowed(
            true,
            true,
            1,
            &pass.retired_market_absent
        ));
        assert!(!retired_market_terminalization_allowed(0, 2, 0, 1, 1));
        let mut paths = vec![];
        let updates = trade.recover_expired_market_absent_orders(&pass, true, true, |id, stamp| {
            fetch_historical_order_trade_audit(id, stamp, |path| {
                paths.push(path.to_string());
                Ok(if path.contains("next_cursor=") {
                    serde_json::json!({"data":[],"next_cursor":"LTE="})
                } else {
                    serde_json::json!({"data":[],"next_cursor":"page2"})
                })
            })
        });
        assert_eq!(paths.len(), 2, "must finish every history page");
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.client_order_id, coid);
        assert_eq!(update.order_slot, order.order_slot);
        assert_eq!(update.symbol, order.token_id);
        assert_eq!(update.side, side);
        assert_eq!(update.status, OrderStatus::Cancelled);
        assert_eq!(
            update.error.as_deref(),
            Some(ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL)
        );
        let final_order = shared.account_state.order(coid).unwrap();
        assert_eq!(final_order.reserved_quantity, 0.0);
        assert_eq!(final_order.reserved_cash, 0.0);
        assert_eq!(final_order.terminal_matched_quantity, Some(0.0));
        assert!(final_order.terminal_trade_ids_authoritative);
        let repeated =
            trade.recover_expired_market_absent_orders(&pass, true, true, |_, _| no_fill());
        assert_eq!(repeated.len(), 1);
        assert_eq!(shared.account_state.order(coid).unwrap(), final_order);
        assert_eq!(
            shared
                .account_state
                .order(&sibling.client_order_id)
                .unwrap()
                .reserved_cash,
            9.28
        );
    }
    finish(trade, shutdown);
}

#[test]
fn expired_absence_requires_clean_cancel_scope_and_complete_history() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    let order = ownership(Side::Sell, "btc01-1789584642112", "0xfirst", "btc01");
    install(&shared, &order);
    for (ended, clean, unrelated_error) in [
        (false, true, false),
        (true, false, false),
        (true, true, true),
    ] {
        let mut pass = absent(&[order.clone()]);
        if unrelated_error {
            pass.errors.push("sibling transport error".into());
        }
        let updates = trade.recover_expired_market_absent_orders(&pass, ended, clean, |_, _| {
            panic!("ineligible scope must not perform history I/O")
        });
        assert!(updates.is_empty());
        assert_eq!(
            shared
                .account_state
                .order(&order.client_order_id)
                .unwrap()
                .reserved_quantity,
            16.0
        );
    }
    for proof in [
        HistoricalOrderTradeAudit::FoundFill { records: 1 },
        HistoricalOrderTradeAudit::Incomplete { pages: 64 },
        HistoricalOrderTradeAudit::Unavailable("timeout".into()),
        HistoricalOrderTradeAudit::Unavailable("malformed pagination".into()),
    ] {
        let mut proof = Some(proof);
        let updates = trade.recover_expired_market_absent_orders(
            &absent(&[order.clone()]),
            true,
            true,
            |_, _| proof.take().unwrap(),
        );
        assert!(updates.is_empty());
        assert_eq!(
            shared
                .account_state
                .order(&order.client_order_id)
                .unwrap()
                .reserved_quantity,
            16.0
        );
    }
    finish(trade, shutdown);
}

#[test]
fn private_match_arriving_during_history_audit_prevents_zero_fill_recovery() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    let order = ownership(Side::Sell, "btc01-1789584642112", "0xfirst", "btc01");
    install(&shared, &order);
    let updates = trade.recover_expired_market_absent_orders(
        &absent(&[order.clone()]),
        true,
        true,
        |_, _| {
            shared
                .account_state
                .apply_authoritative_order_audit(
                    &order.client_order_id,
                    OrderStatus::Cancelled,
                    &AuthoritativeOrderAudit {
                        original_size: Some("16".into()),
                        size_matched: Some("4".into()),
                        associate_trades: vec!["late-trade".into()],
                    },
                )
                .unwrap();
            no_fill()
        },
    );
    assert!(updates.is_empty());
    let current = shared.account_state.order(&order.client_order_id).unwrap();
    assert_eq!(current.terminal_matched_quantity, Some(4.0));
    assert_eq!(current.terminal_trade_ids, vec!["late-trade"]);
    assert!(current.reserved_quantity > 0.0);
    finish(trade, shutdown);
}

#[test]
fn expired_recovery_keeps_incomplete_sibling_and_rejects_wrong_instance() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    let a = ownership(Side::Sell, "btc01-1789584642112", "0xfirst", "btc01");
    let b = ownership(Side::Buy, "btc01-1789584642114", "0xsecond", "btc01");
    install(&shared, &a);
    install(&shared, &b);
    let mut mismatch = absent(&[a.clone()]);
    mismatch.retired_market_absent[0].tracked.instance_id = "sibling".into();
    assert!(trade
        .recover_expired_market_absent_orders(&mismatch, true, true, |_, _| panic!(
            "owner mismatch"
        ))
        .is_empty());
    let updates = trade.recover_expired_market_absent_orders(
        &absent(&[a.clone(), b.clone()]),
        true,
        true,
        |oid, _| {
            if oid == a.order_id {
                no_fill()
            } else {
                HistoricalOrderTradeAudit::Incomplete { pages: 2 }
            }
        },
    );
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].client_order_id, a.client_order_id);
    assert_eq!(
        shared
            .account_state
            .order(&b.client_order_id)
            .unwrap()
            .reserved_cash,
        9.28
    );
    finish(trade, shutdown);
}
