use super::reconcile_identity_tests::{install, ownership};
use super::*;
use crate::http1_pool::Role;

#[test]
fn recovery_queries_distinct_physical_owners_and_accepts_positive_terminal_evidence() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let order = ownership(Side::Sell, "btc01-1789622848819", "0xmissing", "btc01");
    trade
        .shared
        .install_runtime_order_id(
            &order.client_order_id,
            &order.order_id,
            &order.token_id,
            Some(&order),
        )
        .unwrap();
    install(&trade.shared, &order);
    let (transport, requests) = super::super::rtt_probe::probe_http_lane(2);
    trade.shared.bind_recovery_http_transport(transport);
    let server = std::thread::spawn(move || {
        let first = requests.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(first.instance_id, "btc01");
        assert_eq!(first.role, Role::Reconcile);
        assert_eq!(first.excluded_slot, None);
        first.reply_for_test(Ok(serde_json::Value::Null), Some((Role::Reconcile, 0)));
        let second = requests.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(second.instance_id, "btc01");
        assert_eq!(second.excluded_slot, Some(0));
        second.reply_for_test(
            Ok(serde_json::json!({"id":"0xmissing", "status":"CANCELED",
            "original_size":"16", "size_matched":"0", "associate_trades":[]})),
            Some((Role::Reconcile, 1)),
        );
    });
    let pass = trade.reconcile_runtime_open_orders_with_updates();
    assert!(pass.errors.is_empty(), "{:?}", pass.errors);
    assert_eq!(pass.updates.len(), 1);
    assert_eq!(pass.updates[0].client_order_id, order.client_order_id);
    assert_eq!(pass.updates[0].status, OrderStatus::Cancelled);
    assert_eq!(
        trade
            .shared
            .account_state
            .order(&order.client_order_id)
            .unwrap()
            .reserved_quantity,
        0.0
    );
    server.join().unwrap();
    shutdown.request();
    shutdown.finish();
    trade.shared.join_background_workers();
}

#[test]
fn recovery_nulls_wrong_slot_and_disconnected_mailbox_preserve_reservation() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let order = ownership(Side::Sell, "btc01-1789622848819", "0xmissing", "btc01");
    install(&trade.shared, &order);
    let sibling = ownership(Side::Buy, "btc02-1789622848819", "0xsibling", "btc02");
    install(&trade.shared, &sibling);
    let (transport, requests) = super::super::rtt_probe::probe_http_lane(2);
    trade.shared.bind_recovery_http_transport(transport);
    let server = std::thread::spawn(move || {
        for secondary_slot in [1, 0] {
            requests
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .reply_for_test(Ok(serde_json::Value::Null), Some((Role::Reconcile, 0)));
            let second = requests.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(second.excluded_slot, Some(0));
            second.reply_for_test(
                Ok(serde_json::Value::Null),
                Some((Role::Reconcile, secondary_slot)),
            );
        }
    });
    for _ in 0..2 {
        assert!(!matches!(
            trade.fetch_recovery_order_via_owners(&order, &order.order_id),
            FetchOrderResult::Found(_)
        ));
        assert_eq!(
            trade
                .shared
                .account_state
                .order(&order.client_order_id)
                .unwrap()
                .reserved_quantity,
            16.0
        );
        assert_eq!(
            trade
                .shared
                .account_state
                .order(&sibling.client_order_id)
                .unwrap()
                .reserved_cash,
            9.28
        );
    }
    server.join().unwrap();
    // Disconnected mailbox is never replaced with global/fallback HTTP I/O.
    assert!(matches!(
        trade.fetch_recovery_order_via_owners(&order, &order.order_id),
        FetchOrderResult::Unavailable(_)
    ));
    shutdown.request();
    shutdown.finish();
    trade.shared.join_background_workers();
}

#[test]
fn active_unknown_order_cancel_ack_and_complete_history_resolve_without_expiry() {
    exact_cancel_recovery_entry(false);
}

#[test]
fn live_per_order_entry_cancels_unknown_order_and_audits_history_without_expiry() {
    exact_cancel_recovery_entry(true);
}

fn exact_cancel_recovery_entry(per_order: bool) {
    let shutdown = ShutdownToken::new();
    let seeded = super::tests::shutdown_test_trade(shutdown.clone());
    let trade = PolymarketTrade::from_shared(seeded.shared.clone(), "", "btc01");
    let mut order = ownership(Side::Sell, "btc01-1789622848819", "0xmissing", "btc01");
    order.status = OrderStatus::NewOrderTimeout;
    trade
        .shared
        .install_runtime_order_id(
            &order.client_order_id,
            &order.order_id,
            &order.token_id,
            Some(&order),
        )
        .unwrap();
    install(&trade.shared, &order);
    let sibling = ownership(Side::Buy, "btc02-1789622848819", "0xsibling", "btc02");
    install(&trade.shared, &sibling);
    assert!(!trade
        .shared
        .account_state
        .token_event_has_ended(&order.token_id));
    let (transport, requests) = super::super::rtt_probe::probe_http_lane(2);
    trade.shared.bind_recovery_http_transport(transport);
    let server = std::thread::spawn(move || {
        for slot in [0, 1] {
            let req = requests.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(req.instance_id, "btc01");
            assert_eq!(
                req.request_parts_for_test(),
                ("GET", "/data/order/0xmissing", "")
            );
            req.reply_for_test(Ok(serde_json::Value::Null), Some((Role::Reconcile, slot)));
        }
        let req = requests.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(req.instance_id, "btc01");
        assert_eq!(req.role, Role::Cancel);
        assert_eq!(
            req.request_parts_for_test(),
            ("DELETE", "/order", "{\"orderID\":\"0xmissing\"}")
        );
        req.reply_for_test(
            Ok(serde_json::json!({"canceled":["0xmissing"],"not_canceled":{}})),
            Some((Role::Cancel, 0)),
        );
        for cursor in ["page-2", "LTE="] {
            let req = requests.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(req.instance_id, "btc01");
            assert_eq!(req.role, Role::Reconcile);
            assert!(req
                .request_parts_for_test()
                .1
                .starts_with("/data/trades?after="));
            req.reply_for_test(
                Ok(serde_json::json!({"data":[],"next_cursor":cursor})),
                Some((Role::Reconcile, 0)),
            );
        }
    });
    // Audit just this unknown order; the unrelated sibling reservation survives.
    let pass = if per_order {
        RuntimeOrderRecovery {
            updates: trade.reconcile_orphans_via_owners(&[(order.client_order_id.clone(),
                order.token_id.clone(), order.side, order.price, Some(order.order_id.clone()))], &[], &[]),
            errors: vec![],
        }
    } else { trade.reconcile_runtime_open_orders_with_updates() };
    assert!(pass.errors.is_empty(), "{:?}", pass.errors);
    assert_eq!(pass.updates.len(), 1);
    let update = &pass.updates[0];
    assert_eq!(update.client_order_id, order.client_order_id);
    assert_eq!(update.order_slot, order.order_slot);
    assert_eq!(update.status, OrderStatus::Cancelled);
    assert_eq!(
        update.order_audit.as_ref().unwrap().size_matched.as_deref(),
        Some("0")
    );
    assert_eq!(
        trade
            .shared
            .account_state
            .order(&order.client_order_id)
            .unwrap()
            .reserved_quantity,
        0.0
    );
    assert_eq!(
        trade
            .shared
            .account_state
            .order(&sibling.client_order_id)
            .unwrap()
            .reserved_cash,
        9.28
    );
    server.join().unwrap();
    // A lost/duplicate reply never clears a different owner or emits new risk.
    assert!(trade
        .cancel_unknown_order_via_owners(&order, &order.order_id)
        .is_none());
    shutdown.request();
    shutdown.finish();
    trade.shared.join_background_workers();
}

#[test]
fn active_unknown_cancel_requires_exact_ack_and_complete_no_fill_history() {
    let cases = [
        (serde_json::Value::Null, None),
        (
            serde_json::json!({"canceled":[],"not_canceled":{"0xmissing":"order can't be found - already canceled or matched"}}),
            None,
        ),
        (
            serde_json::json!({"canceled":["0xother"],"not_canceled":{}}),
            None,
        ),
        (
            serde_json::json!({"canceled":["0xmissing"],"not_canceled":{"0xmissing":"pending"}}),
            None,
        ),
        (serde_json::json!({"canceled":["0xmissing"]}), None),
        (
            serde_json::json!({"canceled":["0xmissing"],"not_canceled":{}}),
            Some(serde_json::json!({"data":[]})),
        ),
        (
            serde_json::json!({"canceled":["0xmissing"],"not_canceled":{}}),
            Some(
                serde_json::json!({"data":[{"id":"late","taker_order_id":"0xmissing","maker_orders":[],"status":"MATCHED"}],"next_cursor":"LTE="}),
            ),
        ),
        (
            serde_json::json!({"canceled":["0xmissing"],"not_canceled":{}}),
            Some(
                serde_json::json!({"data":[{"id":"failed","taker_order_id":"0xmissing","maker_orders":[],"status":"FAILED"}],"next_cursor":"LTE="}),
            ),
        ),
    ];
    for (ack, history) in cases {
        let shutdown = ShutdownToken::new();
        let trade = super::tests::shutdown_test_trade(shutdown.clone());
        let mut order = ownership(Side::Sell, "btc01-1789622848819", "0xmissing", "btc01");
        order.status = OrderStatus::NewOrderTimeout;
        install(&trade.shared, &order);
        let (transport, requests) = super::super::rtt_probe::probe_http_lane(2);
        trade.shared.bind_recovery_http_transport(transport);
        let server = std::thread::spawn(move || {
            let req = requests.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(req.role, Role::Cancel);
            req.reply_for_test(Ok(ack), Some((Role::Cancel, 0)));
            if let Some(history) = history {
                requests
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap()
                    .reply_for_test(Ok(history), Some((Role::Reconcile, 0)));
            }
        });
        assert!(trade
            .cancel_unknown_order_via_owners(&order, &order.order_id)
            .is_none());
        assert_eq!(
            trade
                .shared
                .account_state
                .order(&order.client_order_id)
                .unwrap()
                .reserved_quantity,
            16.0
        );
        server.join().unwrap();
        shutdown.request();
        shutdown.finish();
        trade.shared.join_background_workers();
    }
}

#[test]
fn active_cancel_proof_rechecks_late_trade_and_full_queue_preserves_reservation() {
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let mut order = ownership(Side::Sell, "btc01-1789622848819", "0xmissing", "btc01");
    order.status = OrderStatus::NewOrderTimeout;
    install(&trade.shared, &order);
    let (transport, requests) = super::super::rtt_probe::probe_http_lane(1);
    let occupied = transport.clone();
    let shared = trade.shared.clone();
    let producer = std::thread::spawn(move || {
        occupied.request(
            &shared,
            "btc01",
            "GET",
            "/data/order/occupied",
            "",
            None,
            None,
        )
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while requests.is_empty() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    trade.shared.bind_recovery_http_transport(transport);
    assert!(trade
        .cancel_unknown_order_via_owners(&order, &order.order_id)
        .is_none());
    requests
        .recv()
        .unwrap()
        .reject_not_sent("test queue occupied");
    producer.join().unwrap();
    assert_eq!(
        trade
            .shared
            .account_state
            .order(&order.client_order_id)
            .unwrap()
            .reserved_quantity,
        16.0
    );
    let result = trade.recover_cancelled_order_after_trade_audit_with(
        &order,
        &order.order_id,
        "test exact ACK",
        |_, _| {
            trade
                .shared
                .account_state
                .apply_authoritative_order_audit(
                    &order.client_order_id,
                    OrderStatus::Cancelled,
                    &AuthoritativeOrderAudit {
                        original_size: Some("16".into()),
                        size_matched: Some("4".into()),
                        associate_trades: vec!["late".into()],
                    },
                )
                .unwrap();
            HistoricalOrderTradeAudit::CompleteNoFill {
                pages: 1,
                after_secs: 1_789_620_000,
            }
        },
    );
    assert!(result.is_none());
    let after = trade
        .shared
        .account_state
        .order(&order.client_order_id)
        .unwrap();
    assert_eq!(after.terminal_matched_quantity, Some(4.0));
    assert_eq!(after.terminal_trade_ids, ["late"]);
    shutdown.request();
    shutdown.finish();
    trade.shared.join_background_workers();
}

#[test]
#[ignore = "cold recovery request-to-owner-commit benchmark; mock venue, no network claims"]
fn benchmark_active_unknown_cancel_recovery() {
    const N: usize = 100;
    let shutdown = ShutdownToken::new();
    let trade = super::tests::shutdown_test_trade(shutdown.clone());
    let (transport, requests) = super::super::rtt_probe::probe_http_lane(2);
    trade.shared.bind_recovery_http_transport(transport);
    let (stop_tx, stop_rx) = crossbeam_channel::bounded(1);
    let server = std::thread::spawn(move || {
        let mut count = 0;
        let mut high_water = 0;
        loop {
            crossbeam_channel::select_biased! {
                recv(stop_rx) -> _ => return (count, high_water),
                recv(requests) -> req => {
                    let req = req.unwrap();
                    high_water = high_water.max(requests.len() + 1);
                    count += 1;
                    let (method, path, body) = req.request_parts_for_test();
                    let reply = if method == "DELETE" {
                        let body: serde_json::Value = serde_json::from_str(body).unwrap();
                        serde_json::json!({"canceled":[body["orderID"]],"not_canceled":{}})
                    } else if path.starts_with("/data/trades?") {
                        serde_json::json!({"data":[],"next_cursor":"LTE="})
                    } else { serde_json::Value::Null };
                    let slot = if req.excluded_slot == Some(0) { 1 } else { 0 };
                    let role = req.role;
                    req.reply_for_test(Ok(reply), Some((role, slot)));
                }
            }
        }
    });
    for active in [false, true] {
        let mut samples = Vec::with_capacity(N);
        let mut resolved = 0;
        for index in 0..N {
            let mut order = ownership(
                Side::Sell,
                &format!(
                    "btc01-{}",
                    1_789_622_848_819u64 + index as u64 + if active { 1000 } else { 0 }
                ),
                &format!("0x{active}-{index}"),
                "btc01",
            );
            order.status = OrderStatus::NewOrderTimeout;
            install(&trade.shared, &order);
            let start = std::time::Instant::now();
            assert!(order_lookup_is_absent(
                &trade.fetch_recovery_order_via_owners(&order, &order.order_id)
            ));
            if active
                && trade
                    .cancel_unknown_order_via_owners(&order, &order.order_id)
                    .is_some()
            {
                resolved += 1;
            }
            samples.push(start.elapsed().as_nanos());
        }
        samples.sort_unstable();
        let q = |p: f64| samples[((N as f64 * p).ceil() as usize).saturating_sub(1)];
        println!("active_cancel={active} n={N} resolved={resolved} p50_ns={} p99_ns={} p999_ns={} max_ns={}", q(0.5),q(0.99),q(0.999),samples[N-1]);
        assert_eq!(resolved, if active { N } else { 0 });
    }
    stop_tx.send(()).unwrap();
    let (requests, high_water) = server.join().unwrap();
    println!("requests={requests} queue_capacity=2 observed_depth_max={high_water} overflow=0");
    assert_eq!(requests, N * 6);
    shutdown.request();
    shutdown.finish();
    trade.shared.join_background_workers();
}

#[test]
fn cancel_transport_failure_keeps_intent_and_original_error_until_authoritative_recovery() {
    let shutdown = ShutdownToken::new();
    let seeded = super::tests::shutdown_test_trade(shutdown.clone());
    let mut trade = PolymarketTrade::from_shared(seeded.shared.clone(), "", "btc01");
    let order = ownership(Side::Sell, "btc01-1789622848820", "0xreset", "btc01");
    trade.shared.install_runtime_order_id(&order.client_order_id, &order.order_id,
        &order.token_id, Some(&order)).unwrap();
    install(&trade.shared, &order);
    for failure in [HttpErr::Transport("Connection reset by peer".into()),
        HttpErr::InvalidResponse("truncated response".into()),
        HttpErr::NotSent("slot changed generation".into())] {
        let expected_error = failure.to_string();
        let update = trade.handle_cancel_reply(Exchange::Polymarket, &order.client_order_id,
            CancelCtx { local_oid: Some(order.order_id.clone()), order_slot: order.order_slot,
                symbol: order.token_id.clone(), side: order.side }, Some(Err(failure)));
        assert_eq!(update.status, OrderStatus::CancelOrderTimeout);
        assert_eq!(update.error.as_deref(), Some(expected_error.as_str()));
        assert_eq!(trade.shared.account_state.order(&order.client_order_id).unwrap().reserved_quantity, 16.0);
    }
    let (transport, requests) = super::super::rtt_probe::probe_http_lane(2);
    trade.shared.bind_recovery_http_transport(transport);
    let server = std::thread::spawn(move || {
        for slot in [0, 1] {
            let req = requests.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(req.instance_id, "btc01");
            req.reply_for_test(Ok(serde_json::Value::Null), Some((Role::Reconcile, slot)));
        }
        let req = requests.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(req.request_parts_for_test(), ("DELETE", "/order", "{\"orderID\":\"0xreset\"}"));
        req.reply_for_test(Ok(serde_json::json!({"canceled":["0xreset"],"not_canceled":{}})), Some((Role::Cancel, 0)));
        let req = requests.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(req.role, Role::Reconcile);
        req.reply_for_test(Ok(serde_json::json!({"data":[],"next_cursor":"LTE="})), Some((Role::Reconcile, 0)));
    });
    let updates = trade.reconcile_orphans_via_owners(&[], &[(order.client_order_id.clone(),order.order_id.clone())], &[]);
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].status, OrderStatus::Cancelled);
    assert_eq!(trade.shared.account_state.order(&order.client_order_id).unwrap().reserved_quantity, 0.0);
    server.join().unwrap();
    shutdown.request(); shutdown.finish(); trade.shared.join_background_workers();
}

#[test]
fn per_order_recovery_rejects_cross_instance_identity_before_http() {
    let shutdown = ShutdownToken::new();
    let seeded = super::tests::shutdown_test_trade(shutdown.clone());
    let trade = PolymarketTrade::from_shared(seeded.shared.clone(), "", "btc01");
    let sibling = ownership(Side::Sell, "btc02-sibling", "0xsibling", "btc02");
    install(&trade.shared, &sibling);
    let (transport, requests) = super::super::rtt_probe::probe_http_lane(2);
    trade.shared.bind_recovery_http_transport(transport);
    let updates = trade.reconcile_orphans_via_owners(&[(sibling.client_order_id.clone(),
        sibling.token_id.clone(), sibling.side, sibling.price, Some(sibling.order_id.clone()))], &[], &[]);
    assert!(updates.is_empty()); assert!(requests.try_recv().is_err());
    assert_eq!(trade.shared.account_state.order(&sibling.client_order_id).unwrap().reserved_quantity, 16.0);
    shutdown.request(); shutdown.finish(); trade.shared.join_background_workers();
}
