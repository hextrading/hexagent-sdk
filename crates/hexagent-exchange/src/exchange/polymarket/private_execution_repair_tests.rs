use super::*;
use serde_json::json;

fn fixture() -> (Arc<SharedState>, PrivateRouteDedupe, Vec<PrivateEventDelta>) {
    let shared = tests::test_shared();
    shared.account_state.register_instance("owner-1", 1.0);
    shared
        .account_state
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    let mut events = Vec::new();
    for index in 0..3 {
        let coid = format!("repair-coid-{index}");
        let oid = format!("0xrepair-{index}");
        let id = format!("repair-trade-{index}");
        shared
            .account_state
            .reserve_order("owner-1", &coid, &oid, "TOKEN", Side::Buy, 2.0, 0.5, 0)
            .unwrap();
        shared.register_order_id(&coid, &oid, "TOKEN");
        events.push(PrivateEventDelta::classify(json!({
            "event_type":"trade", "market":"test-condition", "id":id, "status":"MATCHED", "asset_id":"TOKEN", "side":"BUY",
            "size":"2", "price":"0.5", "taker_order_id":oid,
            "maker_orders":[{"order_id":"other", "asset_id":"TOKEN", "side":"SELL", "matched_amount":"2", "price":"0.5"}]
        })).unwrap());
    }
    for index in 0..2 {
        let coid = format!("repair-coid-{index}");
        let oid = format!("0xrepair-{index}");
        let id = format!("repair-trade-{index}");
        let transition = shared.account_state.apply_trade_transition_with_context(
            &id,
            "MATCHED",
            &coid,
            &oid,
            "TOKEN",
            Side::Buy,
            2.0,
            0.5,
            false,
            123,
        );
        assert!(transition.ownership().is_some());
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    let seeds = loop {
        let seeds = shared.account_state.private_execution_seed();
        if seeds.len() == 2 {
            break seeds;
        }
        assert!(
            Instant::now() < deadline,
            "cold mirror did not publish startup rows"
        );
        std::thread::yield_now();
    };
    assert_eq!(seeds.len(), 2);
    assert!(seeds.iter().all(|seed| seed.execution.is_none()));
    let mut owner = PrivateRouteDedupe::new();
    owner.execution_cache = Some(PrivateExecutionCache::new(seeds).unwrap());
    shared
        .account_state
        .register_token_fee_config_with_settlement(
            &["TOKEN".into()],
            0.07,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while shared
        .account_state
        .private_execution_seed()
        .iter()
        .any(|seed| seed.execution.is_none())
    {
        assert!(
            Instant::now() < deadline,
            "cold metadata attribution did not publish"
        );
        std::thread::yield_now();
    }
    shared.user_feed_health.mark_strategy_consumer_ready();
    (shared, owner, events)
}

fn cold_reply(
    shared: &SharedState,
    owner: &PrivateRouteDedupe,
    routed: RoutedPrivateBatch,
    generation: Option<u64>,
) -> (
    PrivateExecutionRepairReply,
    tokio::sync::oneshot::Receiver<Result<ReplayApplySummary, String>>,
) {
    let (repair_tx, repair_rx) = crossbeam_channel::bounded(1);
    let (ack_tx, _ack_rx) = crossbeam_channel::bounded(1);
    let (completion, mut done) = tokio::sync::oneshot::channel();
    let feedback = PrivateColdFeedback {
        repair_tx,
        ack_tx,
        reconnect_generation: Arc::new(AtomicU64::new(0)),
        reconnect_notify: Arc::new(tokio::sync::Notify::new()),
        execution_ack: Some(owner.execution_cache.as_ref().unwrap().ack_lane()),
    };
    shared.with_test_live_position(|live| {
        apply_private_cold_command(
            shared,
            live,
            &mut PrivateReplayOwner::new(),
            PrivateColdCommand {
                events: routed.events,
                identities: routed.identities,
                durable_skips: routed.durable_skips,
                recovery_generation: generation,
                expected_recovery_certificate: None,
                completion: Some(completion),
                routed_at: crate::latency::Instant::now(),
                feedback,
            },
        )
    });
    assert!(matches!(
        done.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    (repair_rx.try_recv().unwrap(), done)
}

#[test]
fn mixed_new_and_multiple_pending_history_complete_only_after_owner_delivery() {
    let (shared, mut owner, events) = fixture();
    let generation = shared.user_feed_health.begin_recovery_delivery();
    let (tx, rx) = crossbeam_channel::bounded(8);
    let routed = route_private_batch(
        &shared,
        &tx,
        events.clone(),
        Some(generation),
        &mut owner,
        None,
    )
    .unwrap();
    assert!(owner.repair_inflight);
    assert_eq!(
        rx.len(),
        1,
        "normal new fill never waits for historical cold repair"
    );
    assert_eq!(
        routed
            .events
            .iter()
            .filter(|event| event.needs_execution_repair)
            .count(),
        2
    );
    assert!(route_private_batch(
        &shared,
        &tx,
        vec![events[0].clone()],
        Some(generation),
        &mut owner,
        None
    )
    .is_err());
    let (reply, mut done) = cold_reply(&shared, &owner, routed, Some(generation));
    assert_eq!(reply.seeds.len(), 2);
    finish_private_execution_repair(&shared, &tx, &mut owner, reply).unwrap();
    assert!(!owner.repair_inflight);
    assert!(done.try_recv().unwrap().is_ok());
    assert_eq!(rx.len(), 3);
    assert_eq!(
        shared
            .user_feed_health
            .recovery_delivery_progress(generation),
        Some((false, 3))
    );
    for routed in rx.try_iter() {
        assert_eq!(routed.owner, 0);
        assert!(routed.update.trade_fee.is_some());
        assert!(shared
            .user_feed_health
            .acknowledge_recovery_update("owner-1", &routed.update));
    }
    assert!(shared
        .user_feed_health
        .finish_recovery_delivery_enrollment(generation));
    assert_eq!(
        shared
            .user_feed_health
            .recovery_delivery_progress(generation),
        Some((true, 0))
    );
}

#[test]
fn stale_generation_repair_retains_delivery_proof_for_next_gap_even_after_order_gc() {
    let (shared, mut owner, events) = fixture();
    let old = shared.user_feed_health.begin_recovery_delivery();
    let (tx, rx) = crossbeam_channel::bounded(8);
    let routed = route_private_batch(
        &shared,
        &tx,
        vec![events[0].clone()],
        Some(old),
        &mut owner,
        None,
    )
    .unwrap();
    let (reply, mut done) = cold_reply(&shared, &owner, routed, Some(old));
    let _new = shared.user_feed_health.begin_recovery_delivery();
    assert!(finish_private_execution_repair(&shared, &tx, &mut owner, reply).is_err());
    assert!(done.try_recv().unwrap().is_err());
    assert!(rx.is_empty());
    assert!(owner.execution_cache.as_ref().unwrap().needs_delivery(
        "repair-trade-0",
        "0xrepair-0",
        false
    ));
    // Remove the runtime oid publication: the retained cold proof, not this
    // potentially retired mirror, owns the exceptional delivery identity.
    let without_order = tests::test_shared();
    without_order
        .user_feed_health
        .mark_strategy_consumer_ready();
    let next = without_order.user_feed_health.begin_recovery_delivery();
    assert!(without_order.lookup_order_ownership("0xrepair-0").is_none());
    let replay = route_private_batch(
        &without_order,
        &tx,
        vec![events[0].clone()],
        Some(next),
        &mut owner,
        None,
    )
    .unwrap();
    assert_eq!(replay.durable_skips, 0);
    let update = rx.try_recv().unwrap();
    assert_eq!(update.update.client_order_id, "repair-coid-0");
    assert!(update.update.trade_fee.is_some());
    assert!(!owner.execution_cache.as_ref().unwrap().needs_delivery(
        "repair-trade-0",
        "0xrepair-0",
        false
    ));
}

#[test]
fn repair_delivery_backpressure_keeps_exact_fee_and_retryable_owner_proof() {
    let (shared, mut owner, events) = fixture();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let routed = route_private_batch(&shared, &tx, events.clone(), None, &mut owner, None).unwrap();
    assert_eq!(rx.len(), 1);
    let (reply, mut done) = cold_reply(&shared, &owner, routed, None);
    assert!(finish_private_execution_repair(&shared, &tx, &mut owner, reply).is_err());
    assert!(done.try_recv().unwrap().is_err());
    rx.try_recv().unwrap();
    for event in events.into_iter().take(2) {
        let replay =
            route_private_batch(&shared, &tx, vec![event], None, &mut owner, None).unwrap();
        assert_eq!(replay.durable_skips, 0);
        let update = rx.try_recv().unwrap();
        assert!(
            update.update.trade_fee.unwrap().usdc_fee > 0.0
                || update.update.trade_fee.unwrap().shares_fee > 0.0
        );
    }
}

#[test]
fn stale_repair_proof_preserves_later_failed_and_confirmed_delivery() {
    for (venue_status, expected) in [
        ("FAILED", OrderStatus::Failed),
        ("CONFIRMED", OrderStatus::Filled),
    ] {
        let (shared, mut owner, events) = fixture();
        let old = shared.user_feed_health.begin_recovery_delivery();
        let (tx, rx) = crossbeam_channel::bounded(8);
        let routed = route_private_batch(
            &shared,
            &tx,
            vec![events[0].clone()],
            Some(old),
            &mut owner,
            None,
        )
        .unwrap();
        let (reply, mut done) = cold_reply(&shared, &owner, routed, Some(old));
        let new = shared.user_feed_health.begin_recovery_delivery();
        assert!(finish_private_execution_repair(&shared, &tx, &mut owner, reply).is_err());
        assert!(done.try_recv().unwrap().is_err());
        let mut terminal = events[0].clone();
        terminal.payload["status"] = json!(venue_status);
        let routed = route_private_batch(
            &shared,
            &tx,
            vec![terminal.clone()],
            Some(new),
            &mut owner,
            None,
        )
        .unwrap();
        let message = rx.try_recv().unwrap();
        assert_eq!(message.update.status, expected);
        assert!(message.update.trade_fee.is_some());
        apply_private_cold_batch(&shared, &routed.events, Some(new)).unwrap();
        assert_eq!(
            shared
                .account_state
                .trade_ownership("repair-trade-0")
                .unwrap()
                .status,
            venue_status
        );
        let repeated =
            route_private_batch(&shared, &tx, vec![terminal], Some(new), &mut owner, None).unwrap();
        assert!(rx.is_empty());
        assert!(repeated.events.is_empty());
    }
}

#[test]
fn healthy_terminal_repair_cannot_cross_into_new_recovery_without_enrollment() {
    let (shared, mut owner, events) = fixture();
    let certificate = shared.user_feed_health.recovery_certificate();
    let (tx, rx) = crossbeam_channel::bounded(8);
    let routed = route_private_batch(
        &shared,
        &tx,
        vec![events[0].clone()],
        None,
        &mut owner,
        None,
    )
    .unwrap();
    let (mut reply, mut done) = cold_reply(&shared, &owner, routed, None);
    reply.expected_recovery_certificate = Some(certificate);
    shared.user_feed_health.set_recovering(true);
    let generation = shared.user_feed_health.begin_recovery_delivery();
    assert!(finish_private_execution_repair(&shared, &tx, &mut owner, reply).is_err());
    assert!(done.try_recv().unwrap().is_err());
    assert!(rx.is_empty());
    assert!(owner.execution_cache.as_ref().unwrap().needs_delivery(
        "repair-trade-0",
        "0xrepair-0",
        false
    ));
    route_private_batch(
        &shared,
        &tx,
        vec![events[0].clone()],
        Some(generation),
        &mut owner,
        None,
    )
    .unwrap();
    assert_eq!(rx.len(), 1);
    assert_eq!(
        shared
            .user_feed_health
            .recovery_delivery_progress(generation),
        Some((false, 1))
    );
}
