use super::*;

fn feedback() -> (
    PrivateColdFeedback,
    crossbeam_channel::Receiver<PrivateExecutionRepairReply>,
) {
    let (repair_tx, repair_rx) = crossbeam_channel::bounded(1);
    let (ack_tx, _) = crossbeam_channel::bounded(8);
    (
        PrivateColdFeedback {
            ack_tx,
            repair_tx,
            reconnect_generation: Arc::new(AtomicU64::new(0)),
            reconnect_notify: Arc::new(tokio::sync::Notify::new()),
            execution_ack: None,
        },
        repair_rx,
    )
}

#[test]
fn archive_filter_false_positive_returns_to_owner_and_delivers_new_fill_once() {
    let (shared, mut owner, mut events) = super::execution_repair_tests::fixture();
    let mut event = events.pop().unwrap();
    // Model an advisory filter false positive. The cold archive contains no
    // proof, so this normal new fill must return to the private owner route.
    event.needs_archive_lookup = true;
    owner.repair_inflight = true;
    let (feedback, replies) = feedback();
    let (completion, done) = tokio::sync::oneshot::channel();
    let command = PrivateColdCommand {
        events: vec![event.clone()],
        identities: vec![],
        durable_skips: 0,
        recovery_generation: None,
        expected_recovery_certificate: None,
        completion: Some(completion),
        routed_at: crate::latency::Instant::now(),
        feedback,
    };
    assert!(shared.enqueue_archive_lookup(command).is_ok());
    let reply = replies.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(reply.archive_feedback.is_some());
    assert!(reply.events[0].archive_checked);
    let (tx, rx) = crossbeam_channel::bounded(8);
    finish_private_execution_repair(&shared, &tx, &mut owner, reply).unwrap();
    let update = rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(update.owner, 0);
    assert_eq!(update.update.trade_id.as_deref(), Some("repair-trade-2"));
    assert_eq!(update.update.filled_quantity, 2.0);
    assert!(done.blocking_recv().unwrap().is_ok());
    assert!(!owner.repair_inflight);
    event.needs_archive_lookup = false;
    route_private_batch(&shared, &tx, vec![event], None, &mut owner, None).unwrap();
    assert!(
        rx.try_recv().is_err(),
        "replay must not redeliver the original fill"
    );
}

#[test]
fn archive_failure_releases_repair_credit_without_advancing_completion() {
    let (shared, mut owner, events) = super::execution_repair_tests::fixture();
    owner.repair_inflight = true;
    let (feedback, replies) = feedback();
    let generation = feedback.reconnect_generation.clone();
    let (completion, mut done) = tokio::sync::oneshot::channel();
    let command = PrivateColdCommand {
        events,
        identities: vec![],
        durable_skips: 0,
        recovery_generation: None,
        expected_recovery_certificate: None,
        completion: Some(completion),
        routed_at: crate::latency::Instant::now(),
        feedback,
    };
    command.fail_archive(&shared, "history archive lane full".into());
    assert!(done.try_recv().is_err());
    let (tx, rx) = crossbeam_channel::bounded(8);
    assert!(
        finish_private_execution_repair(&shared, &tx, &mut owner, replies.try_recv().unwrap())
            .is_err()
    );
    assert!(!owner.repair_inflight);
    assert!(done.blocking_recv().unwrap().is_err());
    assert_eq!(generation.load(Ordering::Acquire), 1);
    assert!(rx.try_recv().is_err());
}

#[test]
fn archived_maker_replay_roundtrip_never_redelivers_or_changes_balances() {
    use super::super::trade::{ClobVersion, GapReplayConfig, PolymarketTrade};
    use hexagent_account::account::shared_account::SharedAccount;
    let directory = std::env::temp_dir().join(format!(
        "private-archive-integration-{}-{}",
        std::process::id(),
        now_ns()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("account.json");
    {
        let account = SharedAccount::new_persistent("archive-integration", &path).unwrap();
        account.register_instance("owner", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        account.flush_persistence(Duration::from_secs(2)).unwrap();
    }
    // Fold the fixture WAL, then inject a valid historical economic-free proof
    // into a CLOSED synthetic ledger. No venue or live account is accessed.
    drop(SharedAccount::new_persistent("archive-integration", &path).unwrap());
    let mut file: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    file["state"]["settled_token_values"]["TOKEN"] = serde_json::json!(1.0);
    file["state"]["instances"]["owner"]["positions"]["TOKEN"] = serde_json::json!(0.0);
    file["state"]["retired_trade_ownership_tombstones"]["archived-venue:archived-oid"] = serde_json::json!({
        "ownership":{"order_slot":OrderSlot::default(),"account_id":"archive-integration","instance_id":"owner","trade_key":"archived-venue:archived-oid","client_order_id":"old-coid","order_id":"archived-oid","token_id":"TOKEN","side":Side::Buy,"quantity":2.0,"price":0.5,"status":"CONFIRMED"},
        "execution_pricing":null,"is_maker":true,"authenticated_terminal_noop":true,"retired_at_ms":1
    });
    std::fs::write(&path, serde_json::to_vec(&file).unwrap()).unwrap();
    let trade = PolymarketTrade::new_with_pool(
        "api-key",
        "c2VjcmV0",
        "passphrase",
        "0x0000000000000000000000000000000000000000000000000000000000000001",
        false,
        10,
        super::super::signer::SignatureType::Eoa,
        ClobVersion::V2,
        "",
        "http://127.0.0.1:1",
        true,
        "archive-integration",
        "",
        GapReplayConfig::default(),
        Some(&path),
    )
    .unwrap();
    let shared = trade.shared_state();
    shared.install_strategy_owner_routes(HashMap::from([("owner".into(), 0)]));
    shared.user_feed_health.mark_strategy_consumer_ready();
    assert!(shared
        .account_state
        .archived_private_event_hint(true, "archived-venue"));
    assert!(shared
        .account_state
        .trade_ownership("archived-venue:archived-oid")
        .is_none());
    let event=PrivateEventDelta::classify(serde_json::json!({"event_type":"trade","id":"archived-venue","status":"CONFIRMED","asset_id":"TOKEN","side":"BUY","size":"2","price":"0.5","taker_order_id":"other-account","maker_orders":[{"maker_address":shared.order_maker_address,"order_id":"archived-oid","asset_id":"TOKEN","side":"BUY","matched_amount":"2","price":"0.5"}]})).unwrap();
    let mut owner = PrivateRouteDedupe::new();
    let (tx, rx) = crossbeam_channel::bounded(8);
    let routed =
        route_private_batch(&shared, &tx, vec![event.clone()], None, &mut owner, None).unwrap();
    assert!(routed.events[0].needs_archive_lookup);
    assert!(rx.try_recv().is_err());
    let (feedback, replies) = feedback();
    let (completion, done) = tokio::sync::oneshot::channel();
    let command = PrivateColdCommand {
        events: routed.events,
        identities: routed.identities,
        durable_skips: routed.durable_skips,
        recovery_generation: None,
        expected_recovery_certificate: None,
        completion: Some(completion),
        routed_at: crate::latency::Instant::now(),
        feedback,
    };
    assert!(shared.enqueue_private_cold(command).is_ok());
    let reply = replies.recv_timeout(Duration::from_secs(3)).unwrap();
    finish_private_execution_repair(&shared, &tx, &mut owner, reply).unwrap();
    assert!(done.blocking_recv().unwrap().is_ok());
    assert!(rx.try_recv().is_err());
    assert_eq!(
        shared
            .account_state
            .monitoring_snapshot_fast()
            .physical_cash,
        100.0
    );
    assert_eq!(
        shared
            .account_state
            .instance_snapshot("owner")
            .unwrap()
            .cash,
        100.0
    );
    let replay = route_private_batch(&shared, &tx, vec![event], None, &mut owner, None).unwrap();
    assert!(replay.events.is_empty());
    assert!(rx.try_recv().is_err());
    drop(shared);
    drop(trade);
    let _ = std::fs::remove_dir_all(directory);
}
