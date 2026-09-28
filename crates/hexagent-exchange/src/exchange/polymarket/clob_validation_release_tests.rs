use super::*;

const SEED: &str = r#"[{"event_type":"book","asset_id":"up","bids":[{"price":"0.55","size":"10"}],"asks":[{"price":"0.56","size":"11"}],"timestamp":"2000"},{"event_type":"book","asset_id":"down","bids":[{"price":"0.44","size":"11"}],"asks":[{"price":"0.45","size":"10"}],"timestamp":"2000"}]"#;

fn unseeded() -> (ClobLocalBooks, Vec<String>, Instant) {
    let tokens = vec!["up".into(), "down".into()];
    let books = ClobLocalBooks::new(&[CanonicalEventSpec {
        condition_id: "condition".into(),
        up_token: "up".into(),
        down_token: "down".into(),
        tick_size: 0.01,
    }]);
    (books, tokens, Instant::now())
}

fn seeded() -> (ClobLocalBooks, Vec<String>, Instant) {
    let (mut books, tokens, now) = unseeded();
    process_clob_frame(SEED, &mut books, &tokens, now, 2_000_000_000);
    (books, tokens, now)
}

fn book(events: &[MarketEvent]) -> Option<&OrderBookSnapshot> {
    events.iter().find_map(|event| match event {
        MarketEvent::OrderBook(book) => Some(book),
        _ => None,
    })
}

#[test]
fn off_grid_delete_does_not_delay_confirmed_bbo() {
    let (mut books, tokens, now) = seeded();
    let batch = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.55","size":"0","side":"BUY","best_bid":"0.54","best_ask":"0.56"},{"asset_id":"up","price":"0.54","size":"10","side":"BUY","best_bid":"0.54","best_ask":"0.56"},{"asset_id":"up","price":"0.001","size":"0","side":"BUY","best_bid":"0.54","best_ask":"0.56"}],"timestamp":"2001"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(100),
        2_001_000_000,
    );
    assert_eq!(
        book(&batch.events)
            .expect("confirmed on-grid BBO publishes immediately")
            .bids[0]
            .price,
        0.54
    );
    assert!(!books.pending_bbo.contains_key("up"));
}

#[test]
fn resolved_bbo_publishes_even_when_last_frame_did_not_change_top() {
    let (mut books, tokens, now) = seeded();
    let first = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.50","size":"10","side":"BUY","best_bid":"0.54","best_ask":"0.56"}],"timestamp":"2001"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(100),
        2_001_000_000,
    );
    assert!(book(&first.events).is_none());
    assert!(books.pending_bbo.contains_key("up"));
    let resolved = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.55","size":"12","side":"BUY","best_bid":"0.55","best_ask":"0.56"}],"timestamp":"2002"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(300),
        2_002_000_000,
    );
    let released =
        book(&resolved.events).expect("resolved validation must bypass quantity coalescing");
    assert_eq!(released.bids[0].price, 0.55);
    assert_eq!(released.bids[0].quantity, 12.0);
    assert!(resolved.repair_tokens.is_empty());
    assert!(!books.pending_bbo.contains_key("up"));
    let duplicate = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.55","size":"12","side":"BUY","best_bid":"0.55","best_ask":"0.56"}],"timestamp":"2002"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(400),
        2_002_100_000,
    );
    assert!(
        book(&duplicate.events).is_none(),
        "ordinary duplicate quantities remain coalesced"
    );
}

#[test]
fn removed_fine_grid_level_ends_tick_wait_without_changing_tick_authority() {
    let (mut books, tokens, now) = seeded();
    let pending = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.555","size":"10","side":"BUY","best_bid":"0.555","best_ask":"0.56"}],"timestamp":"2001"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(100),
        2_001_000_000,
    );
    assert!(book(&pending.events).is_none());
    let restored = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.555","size":"0","side":"BUY","best_bid":"0.55","best_ask":"0.56"}],"timestamp":"2001"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(300),
        2_001_200_000,
    );
    assert_eq!(
        book(&restored.events)
            .expect("all surviving levels remain on the confirmed grid")
            .bids[0]
            .price,
        0.55
    );
    assert_eq!(books.current_ticks["condition"], Decimal::new(1, 2));
    assert!(!books.pending_bbo.contains_key("up"));
}

#[test]
fn newer_on_grid_delta_does_not_release_surviving_fine_grid_depth() {
    let (mut books, tokens, now) = seeded();
    process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.555","size":"10","side":"BUY","best_bid":"0.555","best_ask":"0.56"}],"timestamp":"2001"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(100),
        2_001_000_000,
    );
    let newer = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.50","size":"12","side":"BUY","best_bid":"0.555","best_ask":"0.56"}],"timestamp":"2002"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(300),
        2_002_000_000,
    );
    assert!(book(&newer.events).is_none());
    assert!(books.pending_bbo["up"].awaiting_tick_change);
    assert!(!books.pending_bbo.contains_key("down"));
    assert_eq!(books.current_ticks["condition"], Decimal::new(1, 2));
}

#[test]
fn same_frame_fine_grid_insert_then_delete_does_not_leave_tick_wait() {
    let (mut books, tokens, now) = seeded();
    let batch = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.555","size":"10","side":"BUY","best_bid":"0.555","best_ask":"0.56"},{"asset_id":"up","price":"0.555","size":"0","side":"BUY","best_bid":"0.55","best_ask":"0.56"}],"timestamp":"2001"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(100),
        2_001_000_000,
    );
    assert!(!books.pending_bbo.contains_key("up"));
    assert!(batch.repair_tokens.is_empty());
    assert_eq!(books.current_ticks["condition"], Decimal::new(1, 2));
}

#[test]
fn invalid_side_off_grid_entry_cannot_delay_valid_top_change() {
    let (mut books, tokens, now) = seeded();
    let batch = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.001","size":"10","side":"INVALID","best_bid":"0.001","best_ask":"0.56"},{"asset_id":"up","price":"0.55","size":"0","side":"BUY","best_bid":"0.54","best_ask":"0.56"},{"asset_id":"up","price":"0.54","size":"12","side":"BUY","best_bid":"0.54","best_ask":"0.56"}],"timestamp":"2001"}"#,
        &mut books,
        &tokens,
        now + Duration::from_micros(100),
        2_001_000_000,
    );
    assert_eq!(book(&batch.events).unwrap().bids[0].price, 0.54);
    assert_eq!(batch.wire.ignored, 1);
    assert!(!books.pending_bbo.contains_key("up"));
}

#[test]
#[ignore = "focused validation-to-publication benchmark with a deterministic wire schedule"]
fn benchmark_clob_validation_publication() {
    const N: usize = 1000;
    let mut parser = ResidentClobParser::new();
    let mut buffer = Vec::with_capacity(4096);
    let mut apply =
        |frame: &str, books: &mut ClobLocalBooks, tokens: &[String], at: Instant, ns: u64| {
            buffer.clear();
            buffer.extend_from_slice(frame.as_bytes());
            process_clob_frame_in_place(
                &mut buffer,
                &mut parser,
                books,
                tokens,
                tokens,
                at,
                ns,
                &mut ClobFramePhaseTimings::default(),
            )
        };
    for (name, first, confirmation) in [
        (
            "off_grid_delete",
            None,
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.55","size":"0","side":"BUY","best_bid":"0.54","best_ask":"0.56"},{"asset_id":"up","price":"0.54","size":"10","side":"BUY","best_bid":"0.54","best_ask":"0.56"},{"asset_id":"up","price":"0.001","size":"0","side":"BUY","best_bid":"0.54","best_ask":"0.56"}],"timestamp":"2001"}"#,
        ),
        (
            "bbo_confirmed_unchanged_top",
            Some(
                r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.50","size":"10","side":"BUY","best_bid":"0.54","best_ask":"0.56"}],"timestamp":"2001"}"#,
            ),
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.55","size":"12","side":"BUY","best_bid":"0.55","best_ask":"0.56"}],"timestamp":"2002"}"#,
        ),
        (
            "tick_repeated_quantity",
            Some(
                r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.555","size":"10","side":"BUY","best_bid":"0.555","best_ask":"0.56"}],"timestamp":"2001"}"#,
            ),
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.555","size":"12","side":"BUY","best_bid":"0.555","best_ask":"0.56"}],"timestamp":"2002"}"#,
        ),
    ] {
        let mut cpu = Vec::with_capacity(N);
        let mut holds = Vec::with_capacity(N);
        for _ in 0..N {
            let (mut books, tokens, now) = unseeded();
            apply(SEED, &mut books, &tokens, now, 2_000_000_000);
            if let Some(first) = first {
                apply(
                    first,
                    &mut books,
                    &tokens,
                    now + Duration::from_micros(100),
                    2_001_000_000,
                );
            }
            let ready_at = now
                + Duration::from_micros(if name == "tick_repeated_quantity" {
                    40_000
                } else {
                    300
                });
            let started = std::time::Instant::now();
            let batch = apply(confirmation, &mut books, &tokens, ready_at, 2_002_000_000);
            let immediate = book(&batch.events).is_some();
            std::hint::black_box(&batch);
            cpu.push(started.elapsed().as_nanos() as u64);
            let hold = if immediate {
                0
            } else {
                let deadline = books
                    .next_deferred_deadline()
                    .map_or(ready_at + CLOB_BBO_SETTLE_INTERVAL, |at| {
                        at.min(ready_at + CLOB_BBO_SETTLE_INTERVAL)
                    });
                let deferred = books.flush_deferred_due(deadline, 2_052_000_000, &tokens);
                if book(&deferred.events).is_some() {
                    deadline.duration_since(ready_at).as_nanos() as u64
                } else {
                    let at = now + CLOB_BOOK_COALESCE_INTERVAL + Duration::from_micros(100);
                    assert!(book(&books.flush_due(at, 2_250_100_000)).is_some());
                    at.duration_since(ready_at).as_nanos() as u64
                }
            };
            holds.push(hold);
        }
        for (boundary, mut samples) in [
            ("resident_confirmation_copy_parse_apply_cpu", cpu),
            ("virtual_confirmation_to_book_publication", holds),
        ] {
            samples.sort_unstable();
            eprintln!("clob_validation case={name} boundary={boundary} n={N} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 schedule=deterministic_no_sleep seed_and_first_frame=excluded", samples[N/2-1], samples[N*99/100-1], samples[N*999/1000-1], samples[N-1]);
        }
    }
}

#[test]
fn repeated_quantity_at_pending_fine_price_does_not_restart_tick_grace() {
    let (mut books, tokens, now) = seeded();
    let frame = r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.555","size":"10","side":"BUY","best_bid":"0.555","best_ask":"0.56"}],"timestamp":"2001"}"#;
    process_clob_frame(frame, &mut books, &tokens, now, 2_001_000_000);
    let due = books.next_deferred_deadline().unwrap();
    for micros in [20_000, 40_000] {
        let batch = process_clob_frame(
            frame,
            &mut books,
            &tokens,
            now + Duration::from_micros(micros),
            2_001_000_000 + micros * 1000,
        );
        assert!(
            book(&batch.events).is_none(),
            "tick ordering still receives its original grace period"
        );
        assert_eq!(books.next_deferred_deadline(), Some(due));
    }
    let new_level = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.554","size":"10","side":"BUY","best_bid":"0.555","best_ask":"0.56"}],"timestamp":"2002"}"#,
        &mut books,
        &tokens,
        now + Duration::from_millis(45),
        2_045_000_000,
    );
    assert!(book(&new_level.events).is_none());
    assert_eq!(
        books.next_deferred_deadline(),
        Some(now + Duration::from_millis(95)),
        "new off-grid evidence retains its full validation window"
    );
}

#[test]
fn deleting_last_level_publishes_empty_sides_without_validation_wait() {
    for (frame, empty_bid, empty_ask) in [
        (
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.55","size":"0","side":"BUY","best_bid":"0","best_ask":"0.56"}],"timestamp":"2001"}"#,
            true,
            false,
        ),
        (
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.56","size":"0","side":"SELL","best_bid":"0.55","best_ask":"1"}],"timestamp":"2001"}"#,
            false,
            true,
        ),
        (
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.55","size":"0","side":"BUY","best_bid":"0","best_ask":"1"},{"asset_id":"up","price":"0.56","size":"0","side":"SELL","best_bid":"0","best_ask":"1"}],"timestamp":"2001"}"#,
            true,
            true,
        ),
        (
            r#"{"event_type":"price_change","price_changes":[{"asset_id":"down","price":"0.44","size":"0","side":"BUY","best_bid":"0","best_ask":"0.45"}],"timestamp":"2001"}"#,
            false,
            true,
        ),
    ] {
        let (mut books, tokens, now) = seeded();
        let batch = process_clob_frame(
            frame,
            &mut books,
            &tokens,
            now + Duration::from_micros(100),
            2_001_000_000,
        );
        let snapshot =
            book(&batch.events).expect("confirmed empty side is an immediate book update");
        assert_eq!(snapshot.symbol, "up");
        assert_eq!(snapshot.bids.is_empty(), empty_bid);
        assert_eq!(snapshot.asks.is_empty(), empty_ask);
        assert!(books.pending_bbo.is_empty());
        assert!(batch.repair_tokens.is_empty());
        assert!(batch.diagnostics.is_empty());
    }
}
