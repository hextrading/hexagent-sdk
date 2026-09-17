use super::*;

pub(super) fn ownership(side: Side, coid: &str, oid: &str, instance: &str) -> OrderOwnership {
    OrderOwnership {
        order_slot: OrderSlot::with_generation(8147, 18),
        account_id: "shutdown-test".into(),
        instance_id: instance.into(),
        client_order_id: coid.into(),
        order_id: oid.into(),
        token_id: format!("{instance}-token"),
        side,
        quantity: 16.0,
        filled_quantity: 0.0,
        terminal_matched_quantity: None,
        terminal_trade_ids: Vec::new(),
        terminal_trade_ids_authoritative: false,
        price: 0.58,
        fee_rate_bps: 0,
                cash_fee_per_share: None,
        reserved_cash: if side == Side::Buy { 9.28 } else { 0.0 },
        reserved_quantity: if side == Side::Sell { 16.0 } else { 0.0 },
        status: OrderStatus::CancelUncertain,
    }
}

pub(super) fn tracked(order: &OrderOwnership) -> TrackedOrder {
    TrackedOrder {
        order_slot: order.order_slot,
        symbol: order.token_id.clone(),
        side: order.side,
        instance_id: order.instance_id.clone(),
    }
}

fn snapshot(order: &OrderOwnership) -> ExecutionStateSnapshot {
    ExecutionStateSnapshot {
        open_orders: ExecutionReadMap::from_hash_map(HashMap::from([(
            order.client_order_id.clone(),
            tracked(order),
        )])),
        coid_to_oid: ExecutionReadMap::from_hash_map(HashMap::from([(
            order.client_order_id.clone(),
            order.order_id.clone(),
        )])),
        oid_to_coid: ExecutionReadMap::from_hash_map(HashMap::from([(
            normalize_order_id(&order.order_id),
            order.client_order_id.clone(),
        )])),
        coid_to_token: ExecutionReadMap::from_hash_map(HashMap::from([(
            order.client_order_id.clone(),
            order.token_id.clone(),
        )])),
    }
}

pub(super) fn install(shared: &SharedState, order: &OrderOwnership) {
    shared
        .account_state
        .register_instance(&order.instance_id, 1.0);
    shared
        .account_state
        .backfill_order_ownership(order)
        .unwrap();
    shared
        .runtime_order_ownership
        .insert(&order.order_id, order.clone())
        .unwrap();
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    shared
        .account_lifecycle_tx
        .send(AccountLifecycleJob::ExecutionState(
            ExecutionStateCommand::BenchmarkPublishOpen {
                client_order_id: order.client_order_id.clone(),
                tracked: tracked(order),
                enqueued_ns: now_ns(),
                completion: done_tx,
            },
        ))
        .unwrap();
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
}

fn assert_identity(update: &OrderUpdate, order: &OrderOwnership) {
    assert_eq!(update.client_order_id, order.client_order_id);
    assert_eq!(
        update.exchange_order_id.as_deref(),
        Some(order.order_id.as_str())
    );
    assert_eq!(update.order_slot, order.order_slot);
    assert_eq!(update.symbol, order.token_id);
    assert_eq!(update.side, order.side);
}

#[test]
fn reconcile_cancel_preserves_sell_and_buy_identity_after_cleanup_and_duplicate_audit() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    let sibling = ownership(Side::Buy, "sibling-coid", "0xabcdef", "sibling");
    install(&shared, &sibling);
    for (side, coid, oid) in [
        (Side::Sell, "btc01-1789317172854", "0xfb0b97b3fe7e97db"),
        (Side::Buy, "buy-coid", "0x123456"),
    ] {
        let order = ownership(side, coid, oid, "btc01");
        install(&shared, &order);
        let audit = AuthoritativeOrderAudit {
            original_size: Some("16".into()),
            size_matched: Some("0".into()),
            associate_trades: vec![],
        };
        for duplicate in [false, true] {
            let identity = shared.reconcile_order_identity(coid, oid).unwrap();
            assert_eq!(identity.instance_id, "btc01");
            let mut updates = Vec::new();
            trade.finish_reconciled_cancel(
                None,
                coid,
                oid,
                identity,
                OrderStatus::Cancelled,
                "CANCELED",
                Some(&audit),
                None,
                &mut updates,
            );
            assert_eq!(updates.len(), 1);
            assert_identity(&updates[0], &order);
            assert_eq!(updates[0].status, OrderStatus::Cancelled);
            assert_eq!(updates[0].order_audit.as_ref(), Some(&audit));
            assert_eq!(
                updates[0].error.as_deref(),
                Some(ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL)
            );
            // A cold owner read is a FIFO barrier after audit/teardown jobs.
            let terminal = shared.account_state.order(coid).unwrap();
            assert_eq!(terminal.reserved_cash, 0.0, "duplicate={duplicate}");
            assert_eq!(terminal.reserved_quantity, 0.0, "duplicate={duplicate}");
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while shared.execution_snapshot().open_orders.contains_key(coid) {
                assert!(std::time::Instant::now() < deadline);
                std::thread::yield_now();
            }
        }
        assert!(shared
            .execution_snapshot()
            .open_orders
            .contains_key(&sibling.client_order_id));
        assert_eq!(
            shared
                .account_state
                .order(&sibling.client_order_id)
                .unwrap()
                .reserved_cash,
            9.28
        );
    }
    shutdown.request();
    shutdown.finish();
    assert_eq!(shared.join_background_workers(), 3);
}

#[test]
fn reconcile_retry_and_filled_pending_audit_keep_original_identity_and_reservations() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    for (side, coid, oid) in [(Side::Sell, "sell", "0x11"), (Side::Buy, "buy", "0x22")] {
        let order = ownership(side, coid, oid, "btc01");
        install(&shared, &order);
        for status in [
            OrderStatus::CancelUncertain,
            OrderStatus::CancelOrderTimeout,
            OrderStatus::Filled,
        ] {
            let identity = shared.reconcile_order_identity(coid, oid).unwrap();
            let mut updates = Vec::new();
            trade.finish_reconciled_cancel(
                None,
                coid,
                oid,
                identity,
                status,
                "LIVE",
                None,
                Some("retry-after=1000".into()),
                &mut updates,
            );
            assert_identity(&updates[0], &order);
            assert!(updates[0].order_audit.is_none());
            if status != OrderStatus::Filled {
                assert_eq!(updates[0].error.as_deref(), Some("retry-after=1000"));
            }
            let retained = shared.account_state.order(coid).unwrap();
            assert_eq!(retained.reserved_cash, order.reserved_cash);
            assert_eq!(retained.reserved_quantity, order.reserved_quantity);
        }
    }
    shutdown.request();
    shutdown.finish();
    assert_eq!(shared.join_background_workers(), 3);
}

#[test]
fn reconcile_identity_rejects_missing_and_cross_instance_or_stale_generation_routes() {
    let order = ownership(Side::Sell, "owner-coid", "0xabc", "owner");
    let execution = snapshot(&order);
    let resolve = |execution: &ExecutionStateSnapshot, order: Option<&OrderOwnership>| {
        validated_reconcile_order_identity("shutdown-test", "owner-coid", "0xabc", execution, order)
    };
    assert_eq!(
        resolve(&execution, None).unwrap_err(),
        "missing_order_ownership"
    );
    for field in [
        "account", "coid", "oid", "token", "side", "instance", "slot",
    ] {
        let mut wrong = order.clone();
        match field {
            "account" => wrong.account_id = "other-account".into(),
            "coid" => wrong.client_order_id = "sibling-coid".into(),
            "oid" => wrong.order_id = "0xdef".into(),
            "token" => wrong.token_id = "wrong-token".into(),
            "side" => wrong.side = Side::Buy,
            "instance" => wrong.instance_id = "sibling".into(),
            "slot" => wrong.order_slot = OrderSlot::with_generation(8147, 19),
            _ => unreachable!(),
        }
        assert!(resolve(&execution, Some(&wrong)).is_err(), "{field}");
    }
    let mut wrong_route = execution.clone();
    wrong_route.oid_to_coid =
        ExecutionReadMap::from_hash_map(HashMap::from([("abc".into(), "sibling-coid".into())]));
    assert_eq!(
        resolve(&wrong_route, Some(&order)).unwrap_err(),
        "contradictory_order_route"
    );
}

#[test]
fn reconcile_identity_resolves_replay_from_retained_ownership_without_open_order() {
    let order = ownership(Side::Sell, "owner-coid", "0xabc", "owner");
    let empty = ExecutionStateSnapshot::default();
    let identity = validated_reconcile_order_identity(
        "shutdown-test",
        "owner-coid",
        "ABC",
        &empty,
        Some(&order),
    )
    .unwrap();
    assert_eq!(identity.side, Side::Sell);
    assert_eq!(identity.symbol, order.token_id);
    assert_eq!(identity.order_slot, order.order_slot);
    assert_eq!(identity.instance_id, "owner");
}

#[test]
fn reconcile_missing_identity_stays_orphan_without_network_or_terminal_update() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let shared = trade.shared_state();
    let updates = trade.reconcile_orphans(
        &[(
            "unknown-place".into(),
            "token".into(),
            Side::Sell,
            0.58,
            Some("0xabc".into()),
        )],
        &[("unknown-cancel".into(), "0xdef".into())],
        &[],
    );
    assert!(updates.is_empty());
    assert_eq!(shared.execution_snapshot().open_orders.len(), 0);
    shutdown.request();
    shutdown.finish();
    assert_eq!(shared.join_background_workers(), 3);
}

#[test]
fn authoritative_recovery_preserves_slot_generation_and_sell_identity() {
    let order = ownership(Side::Sell, "owner-coid", "0xabc", "owner");
    let audit = AuthoritativeOrderAudit {
        original_size: Some("16".into()),
        size_matched: Some("4".into()),
        associate_trades: vec!["trade-1".into()],
    };
    let update = PolymarketTrade::authoritative_recovery_update(
        &order,
        &order.order_id,
        OrderStatus::Cancelled,
        audit,
    );
    assert_identity(&update, &order);
    assert_eq!(update.remaining_quantity, 12.0);
    assert_eq!(
        update.filled_quantity, 0.0,
        "the audit must not fabricate a private fill"
    );
}
