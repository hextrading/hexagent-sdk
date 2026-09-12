use super::*;

const SEED: &str = r#"[
 {"event_type":"book","asset_id":"up","bids":[{"price":"0.55","size":"10"},{"price":"0.01","size":"100"}],"asks":[{"price":"0.56","size":"11"}],"timestamp":"2000"},
 {"event_type":"book","asset_id":"down","bids":[{"price":"0.44","size":"11"}],"asks":[{"price":"0.45","size":"10"},{"price":"0.99","size":"100"}],"timestamp":"2000"}
]"#;
const QUANTITY_FRAME: &str = r#"{"event_type":"price_change","market":"condition","price_changes":[{"asset_id":"up","price":"0.01","size":"4436","side":"BUY","best_bid":"0.55","best_ask":"0.56"},{"asset_id":"down","price":"0.99","size":"4436","side":"SELL","best_bid":"0.44","best_ask":"0.45"}],"timestamp":"2001"}"#;

fn seeded() -> (ClobLocalBooks, Vec<String>, Instant) {
    let tokens = vec!["up".to_string(), "down".to_string()];
    let mut books = ClobLocalBooks::new(&[CanonicalEventSpec {
        condition_id: "condition".into(),
        up_token: "up".into(),
        down_token: "down".into(),
        tick_size: 0.01,
    }]);
    let now = Instant::now();
    let batch = process_clob_frame(SEED, &mut books, &tokens, now, 2_000_000_000);
    assert!(batch.diagnostics.is_empty());
    (books, tokens, now)
}

#[test]
#[ignore = "focused parse+quantity-update benchmark; run with --test-threads=1"]
fn benchmark_clob_quantity_update() {
    let (mut books, tokens, now) = seeded();
    let mut parser = ResidentClobParser::new();
    let mut frame = QUANTITY_FRAME.as_bytes().to_vec();
    let mut run = || {
        frame.copy_from_slice(QUANTITY_FRAME.as_bytes());
        let mut phases = ClobFramePhaseTimings::default();
        let batch = process_clob_frame_in_place(
            &mut frame,
            &mut parser,
            &mut books,
            &tokens,
            &tokens,
            now,
            2_001_000_000,
            &mut phases,
        );
        std::hint::black_box(batch);
    };
    for _ in 0..256 {
        run();
    }
    let n = 100_000;
    let mut samples = Vec::with_capacity(n);
    let (_, allocations, bytes) = clob_test_allocator::count(|| {
        for _ in 0..n {
            let start = std::time::Instant::now();
            run();
            samples.push(start.elapsed().as_nanos() as u64);
        }
    });
    samples.sort_unstable();
    eprintln!("clob quantity update: boundary=resident_parse+apply+batch_drop n={n} allocations={allocations} bytes={bytes} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_hwm=0 overflow=0 (same-thread benchmark)", samples[n/2-1], samples[n*99/100-1], samples[n*999/1000-1], samples[n-1]);
}

#[test]
fn quantity_updates_preserve_wire_order_and_allocate_nothing() {
    let (mut books, tokens, now) = seeded();
    let mut parser = ResidentClobParser::new();
    let mut frame = QUANTITY_FRAME.as_bytes().to_vec();
    // Warm the pre-existing latency stage registry before measuring.
    process_clob_frame_in_place(
        &mut frame,
        &mut parser,
        &mut books,
        &tokens,
        &tokens,
        now,
        2_001_000_000,
        &mut ClobFramePhaseTimings::default(),
    );
    let (_, allocations, bytes) = clob_test_allocator::count(|| {
        for _ in 0..100 {
            frame.copy_from_slice(QUANTITY_FRAME.as_bytes());
            let batch = process_clob_frame_in_place(
                &mut frame,
                &mut parser,
                &mut books,
                &tokens,
                &tokens,
                now,
                2_001_000_000,
                &mut ClobFramePhaseTimings::default(),
            );
            assert!(batch.events.is_empty());
            assert!(batch.diagnostics.is_empty());
            assert_eq!(batch.wire.quantity_only_frames, 1);
        }
    });
    assert_eq!((allocations, bytes), (0, 0));
    assert!(books.token_books["down"].wire_sequence > books.token_books["up"].wire_sequence);
    assert_eq!(
        books.token_books["up"].bids[&Decimal::new(1, 2)],
        Decimal::from(4436)
    );
    // Quantity changes remain dirty and publish through the original coalescer.
    let events = books.flush_due(now + CLOB_BOOK_COALESCE_INTERVAL, 2_002_000_000);
    assert!(!events.is_empty());
}

#[test]
fn quantity_preflight_rejects_whole_frame_without_partial_mutation() {
    let (mut books, _, now) = seeded();
    let before_sequence = books.wire_sequence;
    for invalid in [
        QUANTITY_FRAME.replace(
            "\"size\":\"4436\",\"side\":\"SELL\"",
            "\"size\":\"0\",\"side\":\"SELL\"",
        ),
        QUANTITY_FRAME.replace("\"best_ask\":\"0.45\"", "\"best_ask\":\"0.46\""),
        QUANTITY_FRAME.replace("\"price\":\"0.99\"", "\"price\":\"0.98\""),
        QUANTITY_FRAME.replace("\"side\":\"SELL\"", "\"side\":\"UNKNOWN\""),
    ] {
        let fields: PriceChangeFields<'_> = serde_json::from_str(&invalid).unwrap();
        let mut counters = ClobWireCounters::default();
        assert!(!books.try_apply_quantity_only(&fields, now, 2_001_000_000, &mut counters));
        assert_eq!(books.wire_sequence, before_sequence);
        assert_eq!(
            books.token_books["up"].bids[&Decimal::new(1, 2)],
            Decimal::from(100)
        );
        assert_eq!(counters.price_change_entries, 0);
    }
}

#[test]
fn quantity_updates_with_interleaved_duplicates_keep_last_wire_entry() {
    let (mut books, tokens, now) = seeded();
    let frame = QUANTITY_FRAME.replace("}],\"timestamp\"", ",\"unused\":0}],\"timestamp\"");
    let mut value: serde_json::Value = serde_json::from_str(&frame).unwrap();
    let changes = value["price_changes"].as_array_mut().unwrap();
    let mut last_up = changes[0].clone();
    last_up["size"] = "777".into();
    changes.push(last_up);
    let batch = process_clob_frame(&value.to_string(), &mut books, &tokens, now, 2_001_000_000);
    assert_eq!(batch.wire.quantity_only_frames, 1);
    assert_eq!(
        books.token_books["up"].bids[&Decimal::new(1, 2)],
        Decimal::from(777)
    );
    assert!(books.token_books["up"].wire_sequence > books.token_books["down"].wire_sequence);
    assert_eq!(books.token_books["up"].exchange_timestamp_ns, 2_001_000_000);
}

#[test]
fn quantity_updates_reject_stale_replay_and_isolate_instances() {
    let (mut a, _, now) = seeded();
    let (b, _, _) = seeded();
    let fields: PriceChangeFields<'_> = serde_json::from_str(QUANTITY_FRAME).unwrap();
    let mut counters = ClobWireCounters::default();
    assert!(!a.try_apply_quantity_only(&fields, now, 1_999_000_000, &mut counters));
    assert!(a.try_apply_quantity_only(&fields, now, 2_001_000_000, &mut counters));
    assert!(a.try_apply_quantity_only(&fields, now, 2_001_000_000, &mut counters));
    assert_eq!(
        a.token_books["up"].bids[&Decimal::new(1, 2)],
        Decimal::from(4436)
    );
    assert_eq!(
        b.token_books["up"].bids[&Decimal::new(1, 2)],
        Decimal::from(100)
    );
    a.health_states
        .insert("condition".into(), MarketDataHealthState::Repairing);
    assert!(!a.try_apply_quantity_only(&fields, now, 2_002_000_000, &mut counters));
    a.health_states
        .insert("condition".into(), MarketDataHealthState::Healthy);
    a.token_books.remove("down"); // A reconnect must re-seed every token.
    assert!(!a.try_apply_quantity_only(&fields, now, 2_002_000_000, &mut counters));
}
