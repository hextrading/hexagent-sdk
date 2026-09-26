use super::*;

const FRAME: &[u8] = br#"{"event_type":"best_bid_ask","asset_id":"resident-token-up","best_bid":"0.41","best_ask":"0.42","timestamp":"1790418875231"}"#;

fn fixture() -> (ResidentClobParser, ClobLocalBooks, Vec<String>, Vec<u8>) {
    let specs = [CanonicalEventSpec {
        condition_id: "condition".into(), up_token: "resident-token-up".into(),
        down_token: "resident-token-down".into(), tick_size: 0.01,
    }];
    (ResidentClobParser::new(), ClobLocalBooks::new(&specs),
        vec![specs[0].up_token.clone(), specs[0].down_token.clone()], FRAME.to_vec())
}

#[test]
fn steady_bbo_reuses_batch_storage_and_preserves_canonical_versions() {
    let (mut parser, mut books, tokens, mut input) = fixture();
    let mut batch = ClobParsedBatch::preallocated();
    let pointer = batch.events.as_ptr();
    let capacity = batch.events.capacity();
    for index in 0..512 {
        input.copy_from_slice(FRAME);
        process_clob_frame_in_place_observed_into(&mut input, &mut parser, &mut books,
            &tokens, &tokens, Instant::now(), 1790418875231000000 + index,
            &mut ClobFramePhaseTimings::default(), None, &mut batch);
        assert_eq!(batch.events.as_ptr(), pointer);
        assert_eq!(batch.events.capacity(), capacity);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.wire.best_bid_asks, 1, "wire counters reset per frame");
        assert!(batch.diagnostics.is_empty());
        assert!(matches!(&batch.events[0], MarketEvent::Quote(q)
            if q.symbol == "resident-token-up" && q.bid_price == 0.41));
        for event in batch.events.drain(..) { std::hint::black_box(event); }
    }
    assert_eq!(books.quote_versions.len(), 1);
    // A price-change buffer must not throw away the reader's reserved storage.
    append_canonical_clob_events(&mut batch.events, Vec::new());
    assert_eq!(batch.events.as_ptr(), pointer);
}

#[test]
fn warm_bbo_canonicalization_does_not_clone_role_strings() {
    let (_, mut books, _, _) = fixture();
    let quote = || QuoteTick { exchange: Exchange::Polymarket,
        symbol: "resident-token-up".into(), bid_price: 0.41, bid_qty: 0.0,
        ask_price: 0.42, ask_qty: 0.0, exchange_timestamp_ns: 1, local_timestamp_ns: 1 };
    books.canonicalize_quote_ready(quote());
    let input = quote();
    let (event, allocations, _) = clob_test_allocator::count(|| books.canonicalize_quote_ready(input));
    assert!(matches!(event, Some(MarketEvent::Quote(_))));
    assert_eq!(allocations, 0, "warm canonical keys and Up symbol must be reused");
}

#[test]
#[ignore = "controlled batch allocation A/B; run release, single test thread"]
fn benchmark_clob_bbo_batch_reuse() {
    const N: usize = 20_000;
    for fresh in [true, false] {
        let (mut parser, mut books, tokens, mut input) = fixture();
        let mut batch = ClobParsedBatch::preallocated();
        let mut run = || {
            input.copy_from_slice(FRAME);
            if fresh {
                // Former per-frame empty containers. Canonicalization changes
                // are held constant in this controlled buffer-only comparison.
                batch = ClobParsedBatch { events: Vec::new(), diagnostics: Vec::new(),
                    repair_tokens: Vec::new(), wire: ClobWireCounters::default(),
                    recognized_topic: false, bbo_change_snapshots: 0 };
            }
            process_clob_frame_in_place_observed_into(&mut input, &mut parser, &mut books,
                &tokens, &tokens, Instant::now(), 1790418875231000000,
                &mut ClobFramePhaseTimings::default(), None, &mut batch);
            for event in batch.events.drain(..) { std::hint::black_box(event); }
        };
        for _ in 0..256 { run(); }
        let (_, allocations, bytes) = clob_test_allocator::count(|| run());
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let start = Instant::now(); run(); samples.push(start.elapsed().as_nanos());
        }
        samples.sort_unstable();
        eprintln!("clob_bbo fresh_batch={fresh} n={N} boundary=frame_copy_parse_canonicalize_batch_and_drain unit=ns p50={} p99={} p999={} max={} allocations_per_frame={allocations} allocated_bytes_per_frame={bytes} event_high_water=1 queue_depth=0 overflow=0",
            samples[N/2], samples[N*99/100], samples[N*999/1000], samples[N-1]);
    }
}
