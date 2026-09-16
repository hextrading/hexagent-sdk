//! Same-boundary private-owner route regressions and explicit CPU benchmark.
//! The legacy function below is copied from SDK 10651046 user_feed.rs with only
//! its name changed and the newly required execution field initialized to None.
//! It intentionally uses the current fixture/helper dependencies: this isolates
//! route-function changes, and is not a historical whole-binary comparison.
use super::*;
use serde_json::{json, Value};
use std::hint::black_box;
use std::time::Instant as BenchInstant;

const CONDITION: &str = "route-test-condition";
const TOKEN: &str = "DOWN";
const ORDER: &str = "0xabcdef";

fn route_fixture() -> Arc<SharedState> {
    let shared = tests::test_shared();
    shared.account_state.register_instance("owner-1", 1.0);
    shared
        .account_state
        .apply_physical_snapshot(100.0, HashMap::from([(TOKEN.into(), 100.0)]))
        .unwrap();
    shared
        .account_state
        .register_token_fee_config_with_settlement(
            &[TOKEN.into()],
            0.07,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    shared
        .account_state
        .reserve_order(
            "owner-1",
            "execution-route-order",
            ORDER,
            TOKEN,
            Side::Sell,
            64.0,
            0.16,
            700,
        )
        .unwrap();
    shared.register_order_id("execution-route-order", ORDER, TOKEN);
    shared.user_feed_health.mark_strategy_consumer_ready();
    shared
}

fn route_event(legs: usize, trade_id: &str) -> PrivateEventDelta {
    let (quantity, makers) = match legs {
        1 => (15.0, vec![json!({"order_id":"other-0", "asset_id":TOKEN,"side":"BUY", "matched_amount":"15", "price":"0.16"})]),
        2 => (15.0, vec![
            json!({"order_id":"other-0", "asset_id":TOKEN,"side":"BUY", "matched_amount":"3.27", "price":"0.17"}),
            json!({"order_id":"other-1", "asset_id":TOKEN,"side":"BUY", "matched_amount":"11.73", "price":"0.16"}),
        ]),
        64 => (64.0, (0..64).map(|index| json!({"order_id":format!("other-{index}"),"asset_id":TOKEN,"side":"BUY","matched_amount":"1","price":if index%2==0 {"0.16"} else {"0.17"}})).collect()),
        _ => panic!("unsupported test case"),
    };
    PrivateEventDelta::classify(json!({
        "event_type":"trade", "id":trade_id, "status":"MATCHED", "market":CONDITION,
        "asset_id":TOKEN,"side":"SELL", "size":quantity.to_string(), "price":"0.16",
        "taker_order_id":ORDER,"match_time":(now_ns()/1_000_000_000).to_string(), "maker_orders":makers,
    }))
    .unwrap()
}

fn route_close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() < 1e-9,
        "actual={actual:.12}, expected={expected:.12}"
    );
}

#[test]
fn fast_route_and_existing_cold_lane_share_exact_economics_across_metadata_change() {
    let shared = route_fixture();
    let (tx, rx) = crossbeam_channel::bounded(2);
    let mut owner = PrivateRouteDedupe::new();
    owner.execution_cache = Some(PrivateExecutionCache::new(Vec::new()).unwrap());
    let before = shared.account_state.instance_snapshot("owner-1").unwrap();
    let routed = route_private_batch(
        &shared,
        &tx,
        vec![route_event(2, "actual-price")],
        None,
        &mut owner,
        None,
    )
    .unwrap();
    let message = rx.try_recv().unwrap();
    assert_eq!(message.owner, 0);
    assert_eq!(message.update.client_order_id, "execution-route-order");
    route_close(message.update.avg_fill_price, 0.16218);
    route_close(message.update.trade_fee.unwrap().usdc_fee, 0.14267);
    let certificate = routed.events[0].execution.unwrap();
    assert_eq!(message.update.trade_fee, Some(certificate.fee));
    assert_eq!(message.update.avg_fill_price, certificate.price);
    assert_eq!(
        shared
            .account_state
            .instance_snapshot("owner-1")
            .unwrap()
            .cash,
        before.cash,
        "fast routing cannot synchronously book the cold ledger"
    );
    shared
        .account_state
        .register_token_fee_config_with_settlement(
            &[TOKEN.into()],
            0.08,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    apply_private_cold_batch(&shared, &routed.events, None).unwrap();
    let applied = shared.account_state.instance_snapshot("owner-1").unwrap();
    route_close(applied.cash, before.cash + 2.4327 - 0.14267);
    route_close(applied.positions[TOKEN], before.positions[TOKEN] - 15.0);
    let duplicate = route_private_batch(
        &shared,
        &tx,
        vec![route_event(2, "actual-price")],
        None,
        &mut owner,
        None,
    )
    .unwrap();
    assert!(rx.try_recv().is_err());
    apply_private_cold_batch(&shared, &duplicate.events, None).unwrap();
    route_close(
        shared
            .account_state
            .instance_snapshot("owner-1")
            .unwrap()
            .cash,
        applied.cash,
    );
    let next = route_private_batch(
        &shared,
        &tx,
        vec![route_event(2, "new-after-metadata")],
        None,
        &mut owner,
        None,
    )
    .unwrap();
    let next_message = rx.try_recv().unwrap();
    assert_eq!(next.events[0].execution.unwrap().fee_basis.rate, 0.08);
    assert_ne!(next_message.update.trade_fee, message.update.trade_fee);
}

#[test]
fn bounded_root_update_lane_overflow_retries_same_frozen_execution_once() {
    let shared = route_fixture();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let mut owner = PrivateRouteDedupe::new();
    owner.execution_cache = Some(PrivateExecutionCache::new(Vec::new()).unwrap());
    route_private_batch(
        &shared,
        &tx,
        vec![route_event(2, "first")],
        None,
        &mut owner,
        None,
    )
    .unwrap();
    assert_eq!(tx.len(), 1);
    assert!(route_private_batch(
        &shared,
        &tx,
        vec![route_event(2, "blocked")],
        None,
        &mut owner,
        None
    )
    .is_err());
    assert_eq!(tx.len(), 1);
    assert_eq!(
        rx.try_recv().unwrap().update.trade_id.as_deref(),
        Some("first")
    );
    shared
        .account_state
        .register_token_fee_config_with_settlement(
            &[TOKEN.into()],
            0.08,
            1.0,
            FeeSettlement::CollateralV2,
        )
        .unwrap();
    let retried = route_private_batch(
        &shared,
        &tx,
        vec![route_event(2, "blocked")],
        None,
        &mut owner,
        None,
    )
    .unwrap();
    let delivered = rx.try_recv().unwrap();
    assert_eq!(delivered.update.trade_id.as_deref(), Some("blocked"));
    route_close(delivered.update.trade_fee.unwrap().usdc_fee, 0.14267);
    assert_eq!(retried.events[0].execution.unwrap().fee_basis.rate, 0.07);
    route_private_batch(
        &shared,
        &tx,
        vec![route_event(2, "blocked")],
        None,
        &mut owner,
        None,
    )
    .unwrap();
    assert!(rx.try_recv().is_err());
}

#[test]
fn private_route_rejects_foreign_order_token_without_emitting_message() {
    let shared = route_fixture();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let mut owner = PrivateRouteDedupe::new();
    owner.execution_cache = Some(PrivateExecutionCache::new(Vec::new()).unwrap());
    let mut event = route_event(2, "foreign");
    event.payload["asset_id"] = json!("FOREIGN");
    assert!(route_private_batch(&shared, &tx, vec![event], None, &mut owner, None).is_err());
    assert!(rx.try_recv().is_err());
    assert_eq!(
        shared
            .account_state
            .order("execution-route-order")
            .unwrap()
            .filled_quantity,
        0.0
    );
}

fn assert_terminal_scope_changes_after_actual_enqueue(
    shared: &Arc<SharedState>,
    generation: Option<u64>,
    certificate: u64,
) {
    use std::future::Future;
    use std::task::{Context, Poll};
    let (live_tx, _live_rx) = crossbeam_channel::bounded(1);
    let (replay_tx, replay_rx) = crossbeam_channel::bounded(1);
    let lane = PrivateApplyLane {
        live_tx,
        replay_tx,
        reconnect_generation: Arc::new(AtomicU64::new(0)),
        reconnect_notify: Arc::new(tokio::sync::Notify::new()),
    };
    let mut future = Box::pin(lane.replay_terminal_record(
        route_event(2, "terminal-scope").payload,
        generation,
        certificate,
    ));
    let mut context = Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    assert_eq!(
        replay_rx.len(),
        1,
        "the real endpoint must enqueue before scope changes"
    );
    shared.user_feed_health.begin_recovery_delivery();
    let PrivateApplyCommand::Replay {
        recovery_generation,
        expected_recovery_certificate,
        completion,
        ..
    } = replay_rx.try_recv().unwrap()
    else {
        panic!("terminal endpoint must use the production replay command");
    };
    assert_eq!(recovery_generation, generation);
    assert_eq!(expected_recovery_certificate, Some(certificate));
    // This is the exact helper called by the production dequeue branch before
    // route_private_batch. No test-only mirror of recovery state is involved.
    let result = validate_terminal_replay_scope(
        &shared.user_feed_health,
        expected_recovery_certificate,
        recovery_generation,
    );
    assert!(result.is_err());
    completion
        .send(result.map(|()| ReplayApplySummary::default()))
        .unwrap();
    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(Err(_))
    ));
}

#[test]
fn terminal_replay_healthy_enqueue_cannot_cross_a_new_recovery_scope() {
    let shared = route_fixture();
    let initial = shared.user_feed_health.begin_recovery_delivery();
    assert!(shared
        .user_feed_health
        .finish_recovery_delivery_enrollment(initial));
    assert!(shared
        .user_feed_health
        .try_finish_recovery(shared.user_feed_health.recovery_certificate()));
    assert_eq!(
        shared
            .user_feed_health
            .current_recovery_delivery_generation(),
        Ok(None)
    );
    let certificate = shared.user_feed_health.recovery_certificate();
    assert_terminal_scope_changes_after_actual_enqueue(&shared, None, certificate);
}

#[test]
fn terminal_replay_enqueued_recovery_generation_is_rejected_after_new_epoch() {
    let shared = route_fixture();
    let generation = shared.user_feed_health.begin_recovery_delivery();
    let certificate = shared.user_feed_health.recovery_certificate();
    assert_terminal_scope_changes_after_actual_enqueue(&shared, Some(generation), certificate);
    // Even a refreshed certificate cannot legitimize a stale generation.
    assert!(validate_terminal_replay_scope(
        &shared.user_feed_health,
        Some(shared.user_feed_health.recovery_certificate()),
        Some(generation),
    )
    .is_err());
}

#[test]
fn terminal_replay_current_scope_still_waits_for_owner_ack_after_enrollment() {
    let shared = route_fixture();
    let generation = shared.user_feed_health.begin_recovery_delivery();
    let certificate = shared.user_feed_health.recovery_certificate();
    validate_terminal_replay_scope(
        &shared.user_feed_health,
        Some(certificate),
        Some(generation),
    )
    .unwrap();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let mut route_owner = PrivateRouteDedupe::new();
    route_owner.execution_cache = Some(PrivateExecutionCache::new(Vec::new()).unwrap());
    let mut event = route_event(2, "terminal-enrollment");
    event.payload["status"] = json!("CONFIRMED");
    let cold = route_private_batch(
        &shared,
        &tx,
        vec![event],
        Some(generation),
        &mut route_owner,
        None,
    )
    .unwrap();
    let delivered = rx.try_recv().unwrap();
    assert_eq!(
        shared
            .user_feed_health
            .recovery_delivery_progress(generation),
        Some((false, 1))
    );
    apply_private_cold_batch(&shared, &cold.events, Some(generation)).unwrap();
    assert!(shared
        .user_feed_health
        .finish_recovery_delivery_enrollment(generation));
    assert_eq!(
        shared
            .user_feed_health
            .recovery_delivery_progress(generation),
        Some((true, 1))
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let stopped = AtomicBool::new(false);
        assert!(tokio::time::timeout(
            Duration::from_millis(10),
            wait_for_recovery_delivery(&shared, generation, &tx, &stopped)
        )
        .await
        .is_err());
        assert!(shared.user_feed_health.is_recovering());
        assert!(!shared
            .user_feed_health
            .acknowledge_recovery_update("owner", &delivered.update));
        assert!(shared
            .user_feed_health
            .acknowledge_recovery_update("owner-1", &delivered.update));
        tokio::time::timeout(
            Duration::from_secs(1),
            wait_for_recovery_delivery(&shared, generation, &tx, &stopped),
        )
        .await
        .unwrap()
        .unwrap();
    });
    assert!(shared.user_feed_health.try_finish_recovery(certificate));
}

fn benchmark_quantiles(mut values: Vec<u64>) -> Value {
    values.sort_unstable();
    let n = values.len();
    let at = |q: f64| {
        values[((q * n as f64).ceil() as usize)
            .saturating_sub(1)
            .min(n - 1)]
    };
    json!({"n":n,"p50_ns":at(0.5),"p99_ns":at(0.99),"p999_ns":at(0.999),"max_ns":values[n-1]})
}

#[test]
#[ignore = "actual fast-route CPU benchmark; run explicitly alone with --nocapture --test-threads=1"]
fn private_route_before_after_benchmark() {
    const N: usize = 100_000;
    const WARMUP: usize = 5_000;
    crate::latency::prepare_polymarket_private_stages();
    let shared = route_fixture();
    for (case, legs) in [
        ("single_leg", 1),
        ("receipt_two_legs", 2),
        ("maximum_64_legs", 64),
    ] {
        for hit in [false, true] {
            let mut cache = PrivateExecutionCache::new(Vec::new()).unwrap();
            let mut event = route_event(legs, "warmup");
            for _ in 0..WARMUP {
                black_box(
                    legacy_route_private_event_fast(black_box(&event), black_box(&shared)).unwrap(),
                );
                black_box(
                    route_private_event_fast_owned(
                        black_box(&event),
                        black_box(&shared),
                        black_box(&mut cache),
                    )
                    .unwrap(),
                );
            }
            let mut before = Vec::with_capacity(N);
            let mut after = Vec::with_capacity(N);
            for index in 0..N {
                // Event construction and bounded table rotation are excluded
                // from both timed calls. Fresh paths never hit capacity scans.
                if !hit {
                    if index % 16_000 == 0 {
                        cache = PrivateExecutionCache::new(Vec::new()).unwrap();
                    }
                    event.payload["id"] = json!(format!("benchmark-{index}"));
                }
                let mut old_call = || {
                    let started = BenchInstant::now();
                    let result =
                        legacy_route_private_event_fast(black_box(&event), black_box(&shared))
                            .unwrap();
                    let elapsed = started.elapsed().as_nanos() as u64;
                    black_box(result);
                    before.push(elapsed);
                };
                let mut new_call = || {
                    let started = BenchInstant::now();
                    let result = route_private_event_fast_owned(
                        black_box(&event),
                        black_box(&shared),
                        black_box(&mut cache),
                    )
                    .unwrap();
                    let elapsed = started.elapsed().as_nanos() as u64;
                    black_box(result);
                    after.push(elapsed);
                };
                if index % 2 == 0 {
                    old_call();
                    new_call();
                } else {
                    new_call();
                    old_call();
                }
            }
            println!(
                "PRIVATE_ROUTE_BENCH {}",
                json!({
                    "case":case,"cache":if hit {"hit"} else {"fresh"},"warmup":WARMUP,
                    "legacy":benchmark_quantiles(before),"new":benchmark_quantiles(after),
                    "boundary":"borrowed parsed event -> returned FastPrivateUpdate Vec; drop excluded",
                    "legacy_source":"10651046:route_private_event_fast; current helper dependencies",
                    "queue_depth":null,"queue_overflow":null,
                    "excludes":["JSON decoding","event creation","table rotation","queue delivery","cold ledger","network","quote","dispatch"],
                })
            );
        }
    }
}

fn legacy_route_private_event_fast(
    event: &PrivateEventDelta,
    shared: &SharedState,
) -> std::result::Result<Vec<FastPrivateUpdate>, String> {
    let data = event.payload();
    match event.kind {
        PrivateEventKind::Order => {
            let order_id = required_string(data, &["order_id", "orderID", "id"], "order_id")?;
            let asset_id = required_string(data, &["asset_id", "token_id"], "asset_id")?;
            let side = strict_side(required_string(data, &["side"], "side")?, "side")?;
            let price = strict_number(data.get("price"), "price")?;
            let original_size = strict_number(
                data.get("original_size").or_else(|| data.get("size")),
                "original_size",
            )?;
            let size_matched = strict_number(data.get("size_matched"), "size_matched")?;
            let tolerance = 1e-9_f64.max(original_size.abs() * 1e-8);
            if original_size <= 0.0
                || size_matched < 0.0
                || size_matched > original_size + tolerance
                || price <= 0.0
                || price > 1.0 + 1e-8
            {
                return Err(format!(
                    "invalid order lifecycle economics order_id={order_id}"
                ));
            }
            let ownership = shared.lookup_order_ownership(order_id).ok_or_else(|| {
                format!("unowned private order lifecycle event for order_id `{order_id}`")
            })?;
            if ownership.token_id != asset_id
                || ownership.side != side
                || !close_enough(ownership.quantity, original_size)
                || !close_enough(ownership.price, price)
            {
                return Err(format!(
                    "order lifecycle invariant mismatch coid={} order_id={order_id}",
                    ownership.client_order_id,
                ));
            }
            let lifecycle = required_string(data, &["type"], "type")?;
            let status = if lifecycle.eq_ignore_ascii_case("PLACEMENT") {
                OrderStatus::Accepted
            } else if lifecycle.eq_ignore_ascii_case("UPDATE") {
                if size_matched + tolerance >= original_size {
                    OrderStatus::Filled
                } else if size_matched > tolerance {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::Accepted
                }
            } else if lifecycle.eq_ignore_ascii_case("CANCELLATION")
                || lifecycle.eq_ignore_ascii_case("CANCELLED")
                || lifecycle.eq_ignore_ascii_case("CANCELED")
            {
                OrderStatus::Cancelled
            } else {
                return Err(format!("unsupported order lifecycle type `{lifecycle}`"));
            };
            let associate_trades = match data.get("associate_trades") {
                None => Vec::new(),
                Some(value) => {
                    let values = value.as_array().ok_or_else(|| {
                        "order lifecycle associate_trades is not an array".to_string()
                    })?;
                    let mut trades = Vec::with_capacity(values.len());
                    for value in values {
                        let trade_id = value
                            .as_str()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .ok_or_else(|| {
                                "order lifecycle associate_trades contains invalid id".to_string()
                            })?;
                        if trades.iter().any(|existing| existing == trade_id) {
                            return Err(format!(
                                "order lifecycle associate_trades contains duplicate id `{trade_id}`"
                            ));
                        }
                        trades.push(trade_id.to_string());
                    }
                    trades
                }
            };
            if size_matched > tolerance && associate_trades.is_empty() {
                return Err(format!(
                    "matched order lifecycle is missing associate_trades order_id={order_id}"
                ));
            }
            let rank = match status {
                OrderStatus::Accepted => 1,
                OrderStatus::PartiallyFilled => 2,
                OrderStatus::Filled | OrderStatus::Cancelled => 3,
                _ => 0,
            };
            let identity = PrivateRouteIdentity::TradeLifecycle {
                fingerprint: private_route_fingerprint(b'o', &[order_id]),
                rank,
            };
            let produced_ns = now_ns();
            let mut timing = event.timing;
            timing.private_producer_ns = crate::types::monotonic_now_ns();
            Ok(vec![FastPrivateUpdate {
                execution: None,
                owner: shared
                    .strategy_owner(&ownership.instance_id)
                    .ok_or_else(|| {
                        format!(
                            "private lifecycle instance={} has no numeric strategy owner",
                            ownership.instance_id,
                        )
                    })?,
                update: OrderUpdate {
                    order_slot: ownership.order_slot,
                    client_order_id: ownership.client_order_id,
                    exchange: Exchange::Polymarket,
                    symbol: asset_id.to_string(),
                    side,
                    exchange_order_id: Some(order_id.to_string()),
                    status,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: (original_size - size_matched).max(0.0),
                    avg_fill_price: price,
                    timestamp_ns: produced_ns,
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    trade_fee: None,
                    order_audit: Some(AuthoritativeOrderAudit {
                        original_size: Some(original_size.to_string()),
                        size_matched: Some(size_matched.to_string()),
                        associate_trades,
                    }),
                    error: None,
                },
                identity,
                timing,
            }])
        }
        PrivateEventKind::Trade => {
            let (status_name, status, rank) = private_status(data);
            // REST history commonly returns MATCHED even after the durable
            // ledger has advanced the same trade to MINED/CONFIRMED. Treat a
            // later durable edge as the high-water mark so reconnect replay
            // cannot regress StrategyAccount or enter the cold aggregate path.
            if rank > 0 && status_name != "RETRYING" {
                let high_water_started = crate::latency::Instant::now();
                let covered = trade_lifecycle_is_durably_covered(data, shared);
                crate::latency::record(
                    "polymarket.user.trade_lifecycle_high_water",
                    high_water_started,
                );
                if covered {
                    return Ok(Vec::new());
                }
            }
            let fields_started = crate::latency::Instant::now();
            let role = validate_trade_event(data, shared)?;
            crate::latency::record("polymarket.user.validate_trade_fields", fields_started);
            if rank == 0 || status_name == "RETRYING" {
                return Ok(Vec::new());
            }
            let trade_id = required_string(data, &["id", "trade_id"], "trade_id")?;
            let exchange_timestamp_ns = exchange_event_timestamp_ns(data);
            let failure_reason = private_failure_reason(data);
            let mut routed = Vec::new();
            match role {
                PrivateTradeRole::Maker => {
                    for maker in data
                        .get("maker_orders")
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter(|maker| {
                            maker
                                .get("maker_address")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|address| {
                                    address.eq_ignore_ascii_case(&shared.order_maker_address)
                                })
                        })
                    {
                        let order_id = required_string(maker, &["order_id"], "maker order_id")?;
                        let ownership = match shared.lookup_order_ownership(order_id) {
                            Some(ownership) => ownership,
                            None if matches!(status_name, "CONFIRMED" | "FAILED") => {
                                // Do not reject an authenticated historical
                                // terminal leg before the cold account writer
                                // can inspect it. No strategy update is emitted
                                // here. The cold path still requires either an
                                // exact durable trade tombstone or settlement
                                // proof plus one unique historical token owner;
                                // ambiguity therefore remains fail-closed and
                                // keeps the replay cursor pinned.
                                continue;
                            }
                            None => {
                                return Err(format!(
                                    "unowned maker trade `{trade_id}` order_id `{order_id}`"
                                ));
                            }
                        };
                        let symbol = required_string(maker, &["asset_id"], "maker asset_id")?;
                        let side = strict_side(
                            required_string(maker, &["side"], "maker side")?,
                            "maker side",
                        )?;
                        let quantity =
                            strict_number(maker.get("matched_amount"), "matched_amount")?;
                        let price = strict_number(maker.get("price"), "price")?;
                        if ownership.token_id != symbol || ownership.side != side {
                            return Err(format!(
                                "maker trade ownership mismatch trade={trade_id} order_id={order_id}"
                            ));
                        }
                        let key = format!("{}:{}", trade_id, normalize_order_id(order_id));
                        let produced_ns = now_ns();
                        let mut timing = event.timing;
                        timing.private_producer_ns = crate::types::monotonic_now_ns();
                        routed.push(FastPrivateUpdate {
                            execution: None,
                            owner: shared.strategy_owner(&ownership.instance_id).ok_or_else(
                                || {
                                    format!(
                                    "private maker trade instance={} has no numeric strategy owner",
                                    ownership.instance_id,
                                )
                                },
                            )?,
                            update: OrderUpdate {
                                order_slot: ownership.order_slot,
                                client_order_id: ownership.client_order_id,
                                exchange: Exchange::Polymarket,
                                symbol: symbol.to_string(),
                                side,
                                exchange_order_id: Some(order_id.to_string()),
                                status,
                                liquidity: Some(Liquidity::Maker),
                                filled_quantity: quantity,
                                remaining_quantity: 0.0,
                                avg_fill_price: price,
                                timestamp_ns: produced_ns,
                                exchange_event_timestamp_ns: exchange_timestamp_ns,
                                trade_id: Some(key.clone()),
                                trade_fee: None,
                                order_audit: None,
                                error: failure_reason.clone(),
                            },
                            identity: PrivateRouteIdentity::TradeLifecycle {
                                fingerprint: private_route_fingerprint(b'm', &[trade_id, order_id]),
                                rank,
                            },
                            timing,
                        });
                    }
                }
                PrivateTradeRole::Taker => {
                    let order_id = taker_order_id(data).unwrap_or("");
                    let ownership = match shared.lookup_order_ownership(order_id) {
                        Some(ownership) => ownership,
                        None if matches!(status_name, "CONFIRMED" | "FAILED") => {
                            // `classify_private_trade_role` authenticated this
                            // account as the taker. Defer the strict historical
                            // ownership proof to the cold single writer without
                            // broadcasting an unowned fill to any strategy.
                            return Ok(Vec::new());
                        }
                        None => {
                            return Err(format!(
                                "unowned taker trade `{trade_id}` order_id `{order_id}`"
                            ));
                        }
                    };
                    let symbol = required_string(data, &["asset_id", "token_id"], "asset_id")?;
                    let side = strict_side(required_string(data, &["side"], "side")?, "side")?;
                    let quantity = strict_number(
                        data.get("size").or_else(|| data.get("matched_amount")),
                        "size",
                    )?;
                    let price = strict_number(data.get("price"), "price")?;
                    if ownership.token_id != symbol || ownership.side != side {
                        return Err(format!(
                            "taker trade ownership mismatch trade={trade_id} order_id={order_id}"
                        ));
                    }
                    let produced_ns = now_ns();
                    let mut timing = event.timing;
                    timing.private_producer_ns = crate::types::monotonic_now_ns();
                    routed.push(FastPrivateUpdate {
                        execution: None,
                        owner: shared
                            .strategy_owner(&ownership.instance_id)
                            .ok_or_else(|| {
                                format!(
                                    "private taker trade instance={} has no numeric strategy owner",
                                    ownership.instance_id,
                                )
                            })?,
                        update: OrderUpdate {
                            order_slot: ownership.order_slot,
                            client_order_id: ownership.client_order_id,
                            exchange: Exchange::Polymarket,
                            symbol: symbol.to_string(),
                            side,
                            exchange_order_id: Some(order_id.to_string()),
                            status,
                            liquidity: Some(Liquidity::Taker),
                            filled_quantity: quantity,
                            remaining_quantity: 0.0,
                            avg_fill_price: price,
                            timestamp_ns: produced_ns,
                            exchange_event_timestamp_ns: exchange_timestamp_ns,
                            trade_id: Some(trade_id.to_string()),
                            trade_fee: None,
                            order_audit: None,
                            error: failure_reason,
                        },
                        identity: PrivateRouteIdentity::TradeLifecycle {
                            fingerprint: private_route_fingerprint(b't', &[trade_id]),
                            rank,
                        },
                        timing,
                    });
                }
            }
            Ok(routed)
        }
    }
}
