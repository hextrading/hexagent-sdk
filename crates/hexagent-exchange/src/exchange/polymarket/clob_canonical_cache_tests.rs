use super::*;

// Exact 2026-09-15 09:54:18 UTC triggering frame. Both deletes change the
// BBO; the reinsertion restores the fixture outside that captured frame.
const DELETE: &str = include_str!("clob_canonical_cache_frame.json");
const REINSERT: &str = r#"{"market":"0x2c0f1d8b97a9defe11c509c138b144b719c1fd87319bdd4aed622f352d0470d0","price_changes":[{"asset_id":"9180017968174505883254311266236782074738709757594116133936278198686029923573","price":"0.95","size":"10","side":"SELL","best_bid":"0.94","best_ask":"0.95"},{"asset_id":"90552933688904869618590771649259017535496734456965996067288145117156537772079","price":"0.05","size":"10","side":"BUY","best_bid":"0.05","best_ask":"0.06"}],"timestamp":"1789466058235","event_type":"price_change"}"#;
const SEED: &str = r#"[{"event_type":"book","asset_id":"9180017968174505883254311266236782074738709757594116133936278198686029923573","bids":[{"price":"0.94","size":"10"}],"asks":[{"price":"0.95","size":"10"},{"price":"0.96","size":"11"}],"timestamp":"1789466058234"},{"event_type":"book","asset_id":"90552933688904869618590771649259017535496734456965996067288145117156537772079","bids":[{"price":"0.05","size":"10"},{"price":"0.04","size":"11"}],"asks":[{"price":"0.06","size":"10"}],"timestamp":"1789466058234"}]"#;

fn specs() -> Vec<CanonicalEventSpec> {
    vec![CanonicalEventSpec {
        condition_id: "0x2c0f1d8b97a9defe11c509c138b144b719c1fd87319bdd4aed622f352d0470d0".into(),
        up_token: "9180017968174505883254311266236782074738709757594116133936278198686029923573"
            .into(),
        down_token: "90552933688904869618590771649259017535496734456965996067288145117156537772079"
            .into(),
        tick_size: 0.01,
    }]
}

fn seeded() -> (ClobLocalBooks, Vec<String>, Instant) {
    let mut books = ClobLocalBooks::new(&specs());
    let tokens = vec![
        "9180017968174505883254311266236782074738709757594116133936278198686029923573".into(),
        "90552933688904869618590771649259017535496734456965996067288145117156537772079".into(),
    ];
    let now = Instant::now();
    let batch = process_clob_frame(SEED, &mut books, &tokens, now, 1789466058234000000);
    assert!(batch.diagnostics.is_empty());
    (books, tokens, now)
}

#[test]
#[ignore = "focused captured top-delete/reinsert full-apply allocation benchmark"]
fn benchmark_captured_clob_top_change() {
    let (mut books, tokens, now) = seeded();
    let mut parser = ResidentClobParser::new();
    let mut frame = Vec::with_capacity(DELETE.len().max(REINSERT.len()));
    let mut run = |index| {
        frame.clear();
        frame.extend_from_slice(if index % 2 == 0 {
            DELETE.as_bytes()
        } else {
            REINSERT.as_bytes()
        });
        let mut phases = ClobFramePhaseTimings::default();
        let batch = process_clob_frame_in_place(
            &mut frame,
            &mut parser,
            &mut books,
            &tokens,
            &tokens,
            now,
            1789466058235000000,
            &mut phases,
        );
        assert!(batch.diagnostics.is_empty());
        assert_eq!(batch.wire.quantity_only_frames, 0);
        assert!(batch
            .events
            .iter()
            .any(|event| matches!(event, MarketEvent::OrderBook(_))));
        std::hint::black_box(batch);
    };
    for index in 0..256 {
        run(index);
    }
    let n = 10000;
    let mut samples = Vec::with_capacity(n);
    let (_, allocations, bytes) = clob_test_allocator::count(|| {
        for index in 0..n {
            let start = std::time::Instant::now();
            run(index);
            samples.push(start.elapsed().as_nanos() as u64);
        }
    });
    samples.sort_unstable();
    eprintln!("clob_top_change boundary=resident_parse+full_BBO_change_apply+batch_drop n={n} allocations={allocations} bytes={bytes} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_hwm=0 overflow=0",samples[n/2-1],samples[n*99/100-1],samples[n*999/1000-1],samples[n-1]);
}

#[test]
fn startup_canonical_buffers_are_resident_but_not_publishable_before_a_book() {
    let specifications = specs();
    let books = ClobLocalBooks::new(&specifications);
    let spec = &specifications[0];
    let cached = &books.canonical_books[&spec.condition_id];
    assert_eq!(cached.bids.capacity(), CLOB_BOOK_LEVEL_CAPACITY);
    assert_eq!(cached.asks.capacity(), CLOB_BOOK_LEVEL_CAPACITY);
    assert!(books.canonical_snapshot_for_token(&spec.up_token).is_none());
    assert!(books
        .canonical_snapshot_for_token(&spec.down_token)
        .is_none());
}

#[test]
fn canonical_cache_update_reuses_buffers_and_keeps_complete_independent_depth() {
    let mut cached = empty_canonical_snapshot("up");
    let original_bids = cached.bids.as_ptr();
    let original_asks = cached.asks.as_ptr();
    let mut published = OrderBookSnapshot {
        exchange: Exchange::Polymarket,
        symbol: "up".into(),
        bids: vec![
            PriceLevel {
                price: 0.94,
                quantity: 10.0
            };
            CLOB_BOOK_LEVEL_CAPACITY
        ],
        asks: vec![
            PriceLevel {
                price: 0.96,
                quantity: 11.0
            };
            CLOB_BOOK_LEVEL_CAPACITY
        ],
        exchange_timestamp_ns: 10,
        local_timestamp_ns: 11,
    };
    let (_, allocations, bytes) = clob_test_allocator::count(|| {
        for _ in 0..1000 {
            update_canonical_snapshot(&mut cached, &published);
        }
    });
    assert_eq!((allocations, bytes), (0, 0));
    assert_eq!(cached.bids.as_ptr(), original_bids);
    assert_eq!(cached.asks.as_ptr(), original_asks);
    assert_ne!(cached.bids.as_ptr(), published.bids.as_ptr());
    cached.bids[0].quantity = 99.0;
    assert_eq!(published.bids[0].quantity, 10.0);
    // Incremental deltas can accumulate more levels than a single full-book
    // wire frame. Cache reuse must never introduce truncation at the reserve.
    published.bids.resize(
        CLOB_BOOK_LEVEL_CAPACITY + 1,
        PriceLevel {
            price: 0.93,
            quantity: 12.0,
        },
    );
    update_canonical_snapshot(&mut cached, &published);
    assert_eq!(cached.bids.len(), CLOB_BOOK_LEVEL_CAPACITY + 1);
    assert_eq!(cached.bids.last().unwrap().quantity, 12.0);
    published.bids.truncate(1);
    published.exchange_timestamp_ns = 20;
    published.local_timestamp_ns = 21;
    update_canonical_snapshot(&mut cached, &published);
    assert_eq!(cached.bids.len(), 1);
    assert_eq!(cached.exchange_timestamp_ns, 20);
    assert_eq!(cached.local_timestamp_ns, 21);
    assert!(cached.bids.capacity() > CLOB_BOOK_LEVEL_CAPACITY);
}

#[test]
fn captured_top_deletes_keep_previously_emitted_snapshots_and_latest_cache_distinct() {
    let (mut books, tokens, now) = seeded();
    let MarketEvent::OrderBook(prior) = books.canonical_snapshot_for_token(&tokens[0]).unwrap()
    else {
        panic!()
    };
    assert_eq!(prior.asks[0].price, 0.95);
    let batch = process_clob_frame(DELETE, &mut books, &tokens, now, 1789466058235000000);
    assert!(batch.diagnostics.is_empty());
    let emitted = batch
        .events
        .iter()
        .find_map(|event| match event {
            MarketEvent::OrderBook(book) => Some(book),
            _ => None,
        })
        .unwrap();
    assert_eq!(emitted.symbol, tokens[0]);
    assert_eq!(emitted.bids[0].price, 0.94);
    assert_eq!(emitted.asks[0].price, 0.96);
    assert_eq!(
        prior.asks[0].price, 0.95,
        "already queued snapshots are immutable"
    );
    for token in &tokens {
        let MarketEvent::OrderBook(cached) = books.canonical_snapshot_for_token(token).unwrap()
        else {
            panic!()
        };
        assert_eq!(cached.symbol, emitted.symbol);
        assert_eq!(cached.asks[0].price, emitted.asks[0].price);
        assert_eq!(cached.exchange_timestamp_ns, emitted.exchange_timestamp_ns);
    }
    let _ = process_clob_frame(REINSERT, &mut books, &tokens, now, 1789466058235000001);
    assert_eq!(
        emitted.asks[0].price, 0.96,
        "later cache writes cannot alter returned events"
    );
}

#[test]
fn older_complementary_seed_reemits_the_newest_cached_book() {
    let specifications = specs();
    let spec = &specifications[0];
    let tokens = vec![spec.up_token.clone(), spec.down_token.clone()];
    let mut books = ClobLocalBooks::new(&specifications);
    let now = Instant::now();
    let mut seeds: serde_json::Value = serde_json::from_str(SEED).unwrap();
    let first = seeds[0].to_string();
    let initial = process_clob_frame(&first, &mut books, &tokens, now, 1789466058234000000);
    assert!(initial
        .events
        .iter()
        .any(|event| matches!(event, MarketEvent::OrderBook(_))));
    seeds[1]["timestamp"] = serde_json::json!("1789466058233");
    let second = seeds[1].to_string();
    let complementary = process_clob_frame(&second, &mut books, &tokens, now, 1789466058234000001);
    assert!(books.has_all_seeded(&tokens));
    let book = complementary
        .events
        .iter()
        .find_map(|event| match event {
            MarketEvent::OrderBook(book) => Some(book),
            _ => None,
        })
        .unwrap();
    assert_eq!(book.symbol, spec.up_token);
    assert_eq!(book.exchange_timestamp_ns, 1789466058234000000);
    assert_eq!(book.asks[0].price, 0.95);
}

#[test]
#[ignore = "same-binary alternating old/new canonical cache maintenance benchmark"]
fn benchmark_canonical_cache_maintenance_ab() {
    const N: usize = 20_000;
    for depth in [1, 16, CLOB_BOOK_LEVEL_CAPACITY] {
        let published = OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: specs()[0].up_token.clone(),
            bids: vec![
                PriceLevel {
                    price: 0.94,
                    quantity: 10.0
                };
                depth
            ],
            asks: vec![
                PriceLevel {
                    price: 0.96,
                    quantity: 11.0
                };
                depth
            ],
            exchange_timestamp_ns: 10,
            local_timestamp_ns: 11,
        };
        let mut caches = [
            empty_canonical_snapshot(&published.symbol),
            empty_canonical_snapshot(&published.symbol),
        ];
        for _ in 0..1000 {
            caches[0] = published.clone();
            update_canonical_snapshot(&mut caches[1], &published);
        }
        let mut samples = [Vec::with_capacity(N), Vec::with_capacity(N)];
        let mut totals = [(0, 0); 2];
        for index in 0..N {
            // Alternate execution order within the same process so changes in
            // CPU frequency/load affect both implementations in each pair.
            for offset in 0..2 {
                let mode = (index + offset) % 2;
                let (elapsed, allocations, bytes) = clob_test_allocator::count(|| {
                    let started = std::time::Instant::now();
                    if mode == 0 {
                        caches[mode] = published.clone();
                    } else {
                        update_canonical_snapshot(&mut caches[mode], &published);
                    }
                    std::hint::black_box(&caches[mode]);
                    started.elapsed().as_nanos() as u64
                });
                samples[mode].push(elapsed);
                totals[mode].0 += allocations;
                totals[mode].1 += bytes;
            }
        }
        for mode in 0..2 {
            samples[mode].sort_unstable();
            let s = &samples[mode];
            eprintln!("clob_cache_ab mode={} depth_per_side={depth} boundary=cache_update_including_old_cache_drop n={N} allocations={} bytes={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_hwm=0 overflow=0",
                if mode == 0 { "clone_replace" } else { "reuse" }, totals[mode].0, totals[mode].1,
                s[N/2-1],s[N*99/100-1],s[N*999/1000-1],s[N-1]);
        }
        assert_eq!(totals[1], (0, 0));
        assert_eq!(caches[0].bids.len(), caches[1].bids.len());
        assert_eq!(
            caches[0].asks[depth - 1].quantity,
            caches[1].asks[depth - 1].quantity
        );
    }
}
