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
