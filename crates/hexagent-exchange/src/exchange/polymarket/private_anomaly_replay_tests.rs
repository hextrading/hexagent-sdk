use super::*;
use serde_json::json;

fn fixture() -> (Arc<SharedState>, PrivateEventDelta) {
    let shared = tests::test_shared();
    shared.account_state.register_instance("owner-1", 1.0);
    shared.account_state.register_instance("sibling", 1.0);
    shared
        .account_state
        .apply_physical_snapshot(200.0, HashMap::new())
        .unwrap();
    shared
        .account_state
        .register_token_fee_config_with_settlement(
            &["TOKEN".into()],
            0.07,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    shared
        .account_state
        .reserve_order(
            "owner-1",
            "anomaly-coid",
            "0xanomaly",
            "TOKEN",
            Side::Buy,
            2.0,
            0.5,
            0,
        )
        .unwrap();
    shared.register_order_id("anomaly-coid", "0xanomaly", "TOKEN");
    let event = PrivateEventDelta::classify(json!({
        "event_type":"trade", "market":"test-condition", "id":"anomaly-trade", "status":"CONFIRMED", "asset_id":"TOKEN", "side":"BUY",
        "size":"2", "price":"0.5", "taker_order_id":"0xanomaly",
        "maker_orders":[{"order_id":"other", "asset_id":"TOKEN", "side":"SELL", "matched_amount":"2", "price":"0.5"}]
    })).unwrap();
    (shared, event)
}

#[test]
fn pending_anomalies_bypass_all_replay_skips_without_double_booking() {
    let (shared, event) = fixture();
    let (tx, rx) = crossbeam_channel::bounded(8);
    let mut route = PrivateRouteDedupe::new();
    let first =
        route_private_batch(&shared, &tx, vec![event.clone()], None, &mut route, None).unwrap();
    assert_eq!(first.events.len(), 1);
    let mut replay = PrivateReplayOwner::new();
    shared
        .with_test_live_position(|live| {
            apply_private_cold_batch_owned(&shared, live, &mut replay, &first.events, None)
        })
        .unwrap();
    let mut committed = PrivateRouteDedupe::new();
    for id in first.identities {
        committed.remember(id);
    }
    while rx.try_recv().is_ok() {}
    let owner = shared.account_state.instance_snapshot("owner-1").unwrap();
    let sibling = shared.account_state.instance_snapshot("sibling").unwrap();
    shared.account_state.mark_private_event_anomaly(
        "trade:anomaly-trade",
        "transient startup metadata unavailable",
    );
    assert!(
        shared
            .account_state
            .record_authenticated_terminal_trade_noop(
                "anomaly-trade",
                "CONFIRMED",
                "0xanomaly",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                false
            )
            .ownership()
            .is_none()
    );
    assert_eq!(shared.account_state.ownership_anomalies().len(), 2);
    let second = route_private_batch(
        &shared,
        &tx,
        vec![event.clone()],
        None,
        &mut route,
        Some(&committed),
    )
    .unwrap();
    assert_eq!(
        second.events.len(),
        1,
        "pending anomaly must reach cold validator despite durable and commit caches"
    );
    shared
        .with_test_live_position(|live| {
            apply_private_cold_batch_owned(&shared, live, &mut replay, &second.events, None)
        })
        .unwrap();
    assert!(shared.account_state.ownership_anomalies().is_empty());
    assert!(
        rx.is_empty(),
        "already delivered lifecycle must not be delivered twice"
    );
    assert_eq!(
        shared
            .account_state
            .instance_snapshot("owner-1")
            .unwrap()
            .cash,
        owner.cash
    );
    assert_eq!(
        shared
            .account_state
            .instance_snapshot("sibling")
            .unwrap()
            .cash,
        sibling.cash
    );
    let duplicate = route_private_batch(
        &shared,
        &tx,
        vec![event],
        None,
        &mut route,
        Some(&committed),
    )
    .unwrap();
    assert!(duplicate.events.is_empty());
    assert_eq!(duplicate.durable_skips, 1);
}

#[test]
fn malformed_replay_keeps_anomaly_and_other_trade_isolation() {
    let (shared, event) = fixture();
    apply_private_cold_batch(&shared, &[event.clone()], None).unwrap();
    shared
        .account_state
        .mark_private_event_anomaly("trade:anomaly-trade", "await validated replay");
    shared
        .account_state
        .mark_private_event_anomaly("trade:unrelated", "unrelated failure");
    let mut bad = event.clone();
    bad.payload["size"] = json!("not-a-number");
    let (tx, rx) = crossbeam_channel::bounded(8);
    let mut route = PrivateRouteDedupe::new();
    assert!(route_private_batch(&shared, &tx, vec![bad], None, &mut route, None).is_err());
    assert_eq!(shared.account_state.ownership_anomalies().len(), 2);
    assert!(rx.is_empty());
    let good = route_private_batch(&shared, &tx, vec![event], None, &mut route, None).unwrap();
    apply_private_cold_batch(&shared, &good.events, None).unwrap();
    let anomalies = shared.account_state.ownership_anomalies();
    assert_eq!(anomalies.len(), 1);
    assert!(anomalies.contains_key("private_event:trade:unrelated"));
}

#[test]
fn retired_trade_anomaly_reaches_cold_validation_without_strategy_broadcast() {
    let (shared, event) = fixture();
    apply_private_cold_batch(&shared, &[event.clone()], None).unwrap();
    shared
        .account_state
        .release_order("anomaly-coid", OrderStatus::Filled);
    assert_eq!(
        shared
            .account_state
            .prune_terminal_history(&HashSet::from(["TOKEN".into()])),
        (1, 1)
    );
    shared
        .account_state
        .mark_private_event_anomaly("trade:anomaly-trade", "startup validation retry");
    let (tx, rx) = crossbeam_channel::bounded(8);
    let mut route = PrivateRouteDedupe::new();
    let routed = route_private_batch(&shared, &tx, vec![event], Some(7), &mut route, None).unwrap();
    assert_eq!(routed.events.len(), 1);
    assert_eq!(routed.durable_skips, 0);
    apply_private_cold_batch(&shared, &routed.events, Some(7)).unwrap();
    assert!(shared.account_state.ownership_anomalies().is_empty());
    assert!(rx.is_empty());
}

// Exact pre-change a040b1b helper, retained only for paired measurements.
fn before_anomaly_check(payload: &serde_json::Value, shared: &SharedState) -> bool {
    let status = payload
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim_start_matches("TRADE_STATUS_");
    if status.is_empty() || status == "RETRYING" {
        return false;
    }
    let trade_id = payload
        .get("id")
        .or_else(|| payload.get("trade_id"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|trade_id| !trade_id.is_empty());
    let Some(trade_id) = trade_id else {
        return false;
    };
    let mut owned_maker_leg = false;
    for order in payload
        .get("maker_orders")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|order| {
            order
                .get("maker_address")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|address| address.eq_ignore_ascii_case(&shared.order_maker_address))
        })
    {
        owned_maker_leg = true;
        let Some(order_id) = order
            .get("order_id")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|order_id| !order_id.is_empty())
        else {
            return false;
        };
        let Some(token_id) = order.get("asset_id").and_then(serde_json::Value::as_str) else {
            return false;
        };
        let Some(side) = order
            .get("side")
            .and_then(serde_json::Value::as_str)
            .and_then(|side| strict_side(side, "maker side").ok())
        else {
            return false;
        };
        let Some(quantity) = strict_number(order.get("matched_amount"), "matched_amount").ok()
        else {
            return false;
        };
        let Some(price) = strict_number(order.get("price"), "price").ok() else {
            return false;
        };
        let key = format!("{}:{}", trade_id, normalize_order_id(order_id));
        if !shared.account_state.trade_lifecycle_covers_nonblocking(
            &key, status, order_id, token_id, side, quantity, price, true,
        ) {
            return false;
        }
    }
    if owned_maker_leg {
        return true;
    }
    let Some(order_id) = taker_order_id(payload)
        .map(str::trim)
        .filter(|id| !id.is_empty())
    else {
        return false;
    };
    let Some(token_id) = payload
        .get("asset_id")
        .or_else(|| payload.get("token_id"))
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    let Some(side) = payload
        .get("side")
        .and_then(serde_json::Value::as_str)
        .and_then(|side| strict_side(side, "side").ok())
    else {
        return false;
    };
    let Some(quantity) = strict_number(
        payload
            .get("size")
            .or_else(|| payload.get("matched_amount")),
        "size",
    )
    .ok() else {
        return false;
    };
    let Some(price) = strict_number(payload.get("price"), "price").ok() else {
        return false;
    };
    shared.account_state.trade_lifecycle_covers_nonblocking(
        trade_id, status, order_id, token_id, side, quantity, price, false,
    )
}

#[test]
#[ignore = "focused paired private replay high-water benchmark"]
fn benchmark_healthy_replay_anomaly_check() {
    let (shared, event) = fixture();
    apply_private_cold_batch(&shared, &[event.clone()], None).unwrap();
    const N: usize = 20000;
    let mut before = Vec::with_capacity(N);
    let mut after = Vec::with_capacity(N);
    for i in 0..N {
        for new in [i % 2 == 0, i % 2 != 0] {
            let started = std::time::Instant::now();
            let covered = if new {
                trade_lifecycle_is_durably_covered(std::hint::black_box(event.payload()), &shared)
            } else {
                before_anomaly_check(std::hint::black_box(event.payload()), &shared)
            };
            let elapsed = started.elapsed().as_nanos();
            assert!(std::hint::black_box(covered));
            if new {
                after.push(elapsed)
            } else {
                before.push(elapsed)
            }
        }
    }
    for (label, mut values) in [("before", before), ("after", after)] {
        values.sort_unstable();
        let q = |n: usize, d: usize| values[(N - 1) * n / d];
        eprintln!(
            "healthy_replay_check version={} n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=durable_high_water_lookup_plus_anomaly_snapshot_check",
            label,
            N,
            q(50, 100),
            q(99, 100),
            q(999, 1000),
            values[N - 1]
        );
    }
}
