use super::*;

fn subscription() -> ClobSubscription {
    ClobSubscription {
        tokens: vec!["up".into(), "down".into()],
        canonical_events: vec![CanonicalEventSpec {
            condition_id: "condition".into(),
            up_token: "up".into(),
            down_token: "down".into(),
            tick_size: 0.01,
        }],
        live_bbo_only: true,
        ..Default::default()
    }
}

fn apply(books: &mut ClobLocalBooks, frame: &str) -> ClobParsedBatch {
    process_clob_frame(
        frame,
        books,
        &subscription().tokens,
        Instant::now(),
        10_000_000_000,
    )
}

fn quote(batch: &ClobParsedBatch) -> &QuoteTick {
    batch
        .events
        .iter()
        .find_map(|event| match event {
            MarketEvent::Quote(quote) => Some(quote),
            _ => None,
        })
        .expect("immediate quote")
}

fn assert_prices(batch: &ClobParsedBatch, bid: f64, ask: f64, ts: u64) {
    let q = quote(batch);
    assert_eq!(q.symbol, "up");
    assert!((q.bid_price - bid).abs() < 1e-12);
    assert!((q.ask_price - ask).abs() < 1e-12);
    assert_eq!(q.exchange_timestamp_ns, ts * 1_000_000);
    assert_eq!((q.bid_qty, q.ask_qty), (0.0, 0.0));
    assert!(!batch
        .events
        .iter()
        .any(|e| matches!(e, MarketEvent::OrderBook(_))));
    assert!(batch.repair_tokens.is_empty());
}

#[test]
fn live_bbo_delta_without_snapshot_publishes_and_readies_both_outcomes() {
    let sub = subscription();
    let mut books = ClobLocalBooks::for_subscription(&sub);
    assert!(!books.has_all_seeded(&sub.tokens));
    let batch = apply(
        &mut books,
        r#"{"event_type":"price_change","timestamp":"9000","price_changes":[{"asset_id":"down","price":"0.01","size":"9","side":"BUY","best_bid":"0.39","best_ask":"0.59"}]}"#,
    );
    assert_prices(&batch, 0.41, 0.61, 9000);
    assert!(books.has_all_seeded(&sub.tokens));
    assert!(books.token_books.is_empty() && books.canonical_books.is_empty());
    assert!(books.pending_bbo.is_empty() && books.next_deferred_deadline().is_none());
    assert!(books.quarantined_tokens.is_empty() && books.repair_started_at.is_empty());
    let (tx, _rx) = clob_event_lanes();
    let mut lifecycle = ClobLifecycle::default();
    lifecycle.subscribed();
    let mut health = WsHealth::new(Instant::now());
    let mut metrics = ClobWindowMetrics::new(Instant::now());
    assert!(forward_clob_events(
        batch.events,
        &tx,
        &mut lifecycle,
        &mut health,
        &mut metrics,
        &books,
        &sub.tokens,
        Instant::now()
    ));
    assert!(
        lifecycle.ready,
        "no L2 seed or second token update is required"
    );
}

#[test]
fn live_bbo_mixed_sources_share_one_server_timestamp_cursor() {
    let mut books = ClobLocalBooks::for_subscription(&subscription());
    let first = apply(
        &mut books,
        r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.4","best_ask":"0.6","timestamp":"9002"}"#,
    );
    assert_prices(&first, 0.4, 0.6, 9002);
    for old in [
        r#"{"event_type":"price_change","timestamp":"9001","price_changes":[{"asset_id":"down","price":"0.3","size":"1","side":"BUY","best_bid":"0.3","best_ask":"0.5"}]}"#,
        r#"{"event_type":"best_bid_ask","asset_id":"down","best_bid":"0.3","best_ask":"0.5","timestamp":"9001"}"#,
        r#"{"event_type":"book","asset_id":"down","bids":[{"price":"0.3","size":"10"}],"asks":[{"price":"0.5","size":"10"}],"timestamp":"9001"}"#,
    ] {
        assert!(apply(&mut books, old).events.is_empty());
    }
    let equal = r#"{"event_type":"price_change","timestamp":"9002","price_changes":[{"asset_id":"down","price":"0.01","size":"0","side":"BUY","best_bid":"0.38","best_ask":"0.58"}]}"#;
    assert_prices(&apply(&mut books, equal), 0.42, 0.62, 9002);
    assert_prices(&apply(&mut books, equal), 0.42, 0.62, 9002);
    let newer = apply(
        &mut books,
        r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.43","best_ask":"0.63","timestamp":"9003"}"#,
    );
    assert_prices(&newer, 0.43, 0.63, 9003);
    assert!(books.next_deferred_deadline().is_none());
}

#[test]
fn live_bbo_empty_side_clears_prices_and_invalid_input_cannot_poison_cursor() {
    let mut books = ClobLocalBooks::for_subscription(&subscription());
    assert_prices(
        &apply(
            &mut books,
            r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0","best_ask":"0.6","timestamp":"9000"}"#,
        ),
        0.0,
        0.6,
        9000,
    );
    assert_prices(
        &apply(
            &mut books,
            r#"{"event_type":"best_bid_ask","asset_id":"down","best_bid":"0","best_ask":"0.7","timestamp":"9001"}"#,
        ),
        0.3,
        1.0,
        9001,
    );
    for invalid in [
        r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.6","best_ask":"0.4","timestamp":"9500"}"#,
        r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.4","timestamp":"9500"}"#,
        r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.4","best_ask":"0.6"}"#,
        r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.4","best_ask":"0.6","timestamp":"13000"}"#,
        r#"{"event_type":"best_bid_ask","asset_id":"foreign","best_bid":"0.4","best_ask":"0.6","timestamp":"9500"}"#,
    ] {
        assert!(apply(&mut books, invalid).events.is_empty());
    }
    assert_prices(
        &apply(
            &mut books,
            r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.4","best_ask":"0.6","timestamp":"9002"}"#,
        ),
        0.4,
        0.6,
        9002,
    );
}

#[test]
fn live_bbo_snapshot_extracts_only_top_and_same_frame_uses_last_wire_entry() {
    let mut books = ClobLocalBooks::for_subscription(&subscription());
    assert_prices(
        &apply(
            &mut books,
            r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.2","size":"9"},{"price":"0.4","size":"1"}],"asks":[{"price":"0.8","size":"3"},{"price":"0.6","size":"2"}],"timestamp":"9000"}"#,
        ),
        0.4,
        0.6,
        9000,
    );
    let frame = r#"{"event_type":"price_change","timestamp":"9001","price_changes":[{"asset_id":"up","price":"0.01","size":"1","side":"BUY","best_bid":"0.41","best_ask":"0.61"},{"asset_id":"down","price":"0.01","size":"1","side":"BUY","best_bid":"0.38","best_ask":"0.58"}]}"#;
    let batch = apply(&mut books, frame);
    assert_prices(&batch, 0.42, 0.62, 9001);
    assert_eq!(
        batch
            .events
            .iter()
            .filter(|e| matches!(e, MarketEvent::Quote(_)))
            .count(),
        1
    );
    assert!(books.token_books.is_empty());
    assert_prices(
        &apply(
            &mut books,
            r#"{"event_type":"book","asset_id":"up","bids":[],"asks":[],"timestamp":"9002"}"#,
        ),
        0.0,
        1.0,
        9002,
    );
}

#[test]
fn live_bbo_reconnect_and_subscription_isolation_do_not_inherit_foreign_prices() {
    let mut sub = subscription();
    let mut books = ClobLocalBooks::for_subscription(&sub);
    let frame = r#"{"event_type":"best_bid_ask","asset_id":"down","best_bid":"0.4","best_ask":"0.6","timestamp":"9000"}"#;
    apply(&mut books, frame);
    let mut reconnect = ClobLocalBooks::for_subscription(&sub);
    assert!(!reconnect.has_all_seeded(&sub.tokens));
    assert_prices(&apply(&mut reconnect, frame), 0.4, 0.6, 9000);
    sub.canonical_events[0].up_token = "other-up".into();
    sub.canonical_events[0].down_token = "other-down".into();
    sub.tokens = vec!["other-up".into(), "other-down".into()];
    let mut other = ClobLocalBooks::for_subscription(&sub);
    assert!(apply(&mut other, frame).events.is_empty());
    assert!(!other.has_all_seeded(&sub.tokens));
    assert!(books.has_all_seeded(&subscription().tokens));
}

#[test]
fn live_bbo_handoff_retains_newest_cursor_and_rejects_late_candidate_updates() {
    let sub = subscription();
    let mut active = ClobLocalBooks::for_subscription(&sub);
    let mut candidate = ClobLocalBooks::for_subscription(&sub);
    apply(&mut active, r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.45","best_ask":"0.65","timestamp":"9002"}"#);
    apply(&mut candidate, r#"{"event_type":"best_bid_ask","asset_id":"down","best_bid":"0.4","best_ask":"0.6","timestamp":"9000"}"#);
    assert_eq!(candidate.inherit_newer_live_bbo(&active), 1);
    assert_eq!(candidate.inherit_newer_live_bbo(&active), 0, "idempotent handoff");
    let events = candidate.live_checkpoints(&sub.tokens);
    let MarketEvent::Quote(q) = &events[0] else { panic!("quote") };
    assert_eq!((q.bid_price, q.ask_price), (0.45, 0.65));
    assert_eq!((q.exchange_timestamp_ns, q.local_timestamp_ns), (9_002_000_000, 10_000_000_000));
    assert!(apply(&mut candidate, r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.4","best_ask":"0.6","timestamp":"9001"}"#).events.is_empty());
    assert_prices(&apply(&mut candidate, r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.46","best_ask":"0.66","timestamp":"9003"}"#), 0.46, 0.66, 9003);
    assert_eq!(candidate.inherit_newer_live_bbo(&active), 0, "newer candidate wins");
}

#[test]
fn live_bbo_handoff_isolates_conditions_and_preserves_empty_side_health() {
    let mut sub = subscription();
    let mut active = ClobLocalBooks::for_subscription(&sub);
    apply(&mut active, r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0","best_ask":"0.6","timestamp":"9002"}"#);
    let mut candidate = ClobLocalBooks::for_subscription(&sub);
    apply(&mut candidate, r#"{"event_type":"best_bid_ask","asset_id":"up","best_bid":"0.4","best_ask":"0.6","timestamp":"9000"}"#);
    assert_eq!(candidate.inherit_newer_live_bbo(&active), 1);
    assert!(matches!(&candidate.live_checkpoints(&sub.tokens)[1], MarketEvent::MarketDataHealth(h) if h.state == MarketDataHealthState::Degraded));
    sub.canonical_events[0].condition_id = "foreign".into();
    let mut other = ClobLocalBooks::for_subscription(&sub);
    assert_eq!(other.inherit_newer_live_bbo(&active), 0);
    assert!(!other.has_all_seeded(&sub.tokens));
}

#[test]
fn live_bbo_prewarmed_handoff_preserves_server_and_receive_clock_without_next_frame() {
    let sub = subscription();
    let mut books = ClobLocalBooks::for_subscription(&sub);
    let mut logical = ClobSubscription::default();
    let mut reconnect = sub.clone();
    apply(
        &mut books,
        r#"{"event_type":"best_bid_ask","asset_id":"down","best_bid":"0.4","best_ask":"0.6","timestamp":"9000"}"#,
    );
    assert!(commit_preseeded_clob_subscription(
        &mut logical,
        &sub,
        &mut reconnect,
        &books,
        &sub,
        true
    ));
    assert!(logical.live_bbo_only && reconnect.live_bbo_only);
    let events = books.live_checkpoints(&logical.tokens);
    assert_eq!(events.len(), 2);
    let MarketEvent::Quote(q) = &events[0] else {
        panic!("price checkpoint first")
    };
    assert_eq!(q.exchange_timestamp_ns, 9_000_000_000);
    assert_eq!(q.local_timestamp_ns, 10_000_000_000);
    assert!(
        matches!(&events[1], MarketEvent::MarketDataHealth(h) if h.state == MarketDataHealthState::Healthy)
    );
    assert!(books.live_checkpoints(&["foreign".into()]).is_empty());
}

#[test]
#[ignore = "focused before/after parse and publish benchmark"]
fn live_bbo_parse_publish_benchmark() {
    const N: usize = 20_000;
    crate::latency::prepare_polymarket_clob_stages();
    let seed = r#"[{"event_type":"book","asset_id":"up","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}],"timestamp":"9000"},{"event_type":"book","asset_id":"down","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}],"timestamp":"9000"}]"#;
    let frames = [
        r#"{"event_type":"price_change","timestamp":"9001","price_changes":[{"asset_id":"up","price":"0.41","size":"10","side":"BUY","best_bid":"0.41","best_ask":"0.6"}]}"#,
        r#"{"event_type":"price_change","timestamp":"9001","price_changes":[{"asset_id":"up","price":"0.41","size":"0","side":"BUY","best_bid":"0.4","best_ask":"0.6"}]}"#,
    ];
    for live in [false, true] {
        let mut sub = subscription();
        sub.live_bbo_only = live;
        let mut books = ClobLocalBooks::for_subscription(&sub);
        apply(&mut books, seed);
        let mut parser = ResidentClobParser::new();
        let mut buffer = Vec::with_capacity(4096);
        let mut batch = ClobParsedBatch::preallocated();
        let mut times = Vec::with_capacity(N);
        let mut run = |i: usize| {
            let started = Instant::now();
            buffer.clear();
            buffer.extend_from_slice(frames[i % 2].as_bytes());
            process_clob_frame_in_place_observed_into(
                &mut buffer,
                &mut parser,
                &mut books,
                &sub.tokens,
                &sub.tokens,
                started,
                10_000_000_000,
                &mut ClobFramePhaseTimings::default(),
                None,
                &mut batch,
            );
            std::hint::black_box(&batch);
            let ns = started.elapsed().as_nanos() as u64;
            assert!(batch
                .events
                .iter()
                .any(|e| matches!(e, MarketEvent::Quote(_) | MarketEvent::OrderBook(_))));
            if i >= 1000 {
                times.push(ns);
            }
        };
        for i in 0..1000 {
            run(i);
        }
        let (_, allocations, bytes) = clob_test_allocator::count(|| {
            for i in 1000..N + 1000 {
                run(i);
            }
        });
        times.sort_unstable();
        eprintln!("live_bbo_benchmark mode={} boundary=resident_copy_parse_apply_publish n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} allocations={} bytes={} queue_depth=0 overflow=0 synthetic_direct_call=true",
            if live {"direct_bbo"} else {"previous_full_depth"}, N, times[N/2-1],times[N*99/100-1],times[N*999/1000-1],times[N-1], allocations, bytes);
    }
    // No consumer runs in this focused test. A remaining slot proves none of
    // the measured observations overflowed. This untimed probe deliberately
    // fills the rest and causes exactly one metrics-only rejection.
    let mut remaining = 0;
    while crate::latency::observe_ns("polymarket.ws.clob_live_bbo_apply", 1) {
        remaining += 1;
    }
    assert!(
        remaining > 0,
        "measured telemetry must not fill its bounded lane"
    );
    eprintln!("live_bbo_benchmark telemetry_remaining_before_capacity_probe={remaining} measured_overflow=0 capacity_probe_overflow=1");
}
