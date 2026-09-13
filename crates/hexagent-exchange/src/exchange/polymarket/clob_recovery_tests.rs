use super::*;

#[test]
fn bbo_history_is_bounded_ordered_and_allocation_free() {
    let mut history = BboFrameHistory::default();
    let (_, allocations, bytes) = clob_test_allocator::count(|| {
        for i in 0..10_000 {
            history.push(BboFrameSample {
                exchange_timestamp_ns: i,
                entries: 2,
                expected: ReportedBbo {
                    bid: None,
                    ask: Some(None),
                },
                actual: (Some(Decimal::new(4, 1)), None),
            });
        }
    });
    assert_eq!((allocations, bytes), (0, 0));
    assert_eq!(history.0.len(), CLOB_BBO_DIAGNOSTIC_FRAMES);
    assert_eq!(
        history
            .0
            .iter()
            .map(|s| s.exchange_timestamp_ns)
            .collect::<Vec<_>>(),
        vec![9996, 9997, 9998, 9999]
    );
    let detail = format!("{history:?}");
    assert!(detail.contains("ts=9996 entries=2 expected_bid=None expected_ask=Some(None) actual_bid=Some(0.4) actual_ask=None"));
}

fn subscription(tokens: &[&str]) -> ClobSubscription {
    ClobSubscription {
        tokens: tokens.iter().map(|s| s.to_string()).collect(),
        canonical_events: vec![],
    }
}

#[test]
fn subset_cutover_retains_actual_wire_generation_and_hot_failover() {
    let wire = subscription(&["old", "next"]);
    let mut reconnect = wire.clone();
    let mut logical = subscription(&["old"]);
    let mut books = ClobLocalBooks::default();
    let now = Instant::now();
    process_clob_frame(
        r#"[{"event_type":"book","asset_id":"old","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}],"timestamp":"2000"},{"event_type":"book","asset_id":"next","bids":[{"price":"0.4","size":"10"}],"asks":[{"price":"0.6","size":"10"}],"timestamp":"2000"}]"#,
        &mut books,
        &wire.tokens,
        now,
        2_000_000_000,
    );
    let connected_generation = clob_token_generation(&wire.tokens);
    assert!(commit_preseeded_clob_subscription(
        &mut logical,
        &wire,
        &mut reconnect,
        &books,
        &subscription(&["next"]),
        false
    ));
    assert_eq!(logical.tokens, ["old"]);
    assert!(commit_preseeded_clob_subscription(
        &mut logical,
        &wire,
        &mut reconnect,
        &books,
        &subscription(&["next"]),
        true
    ));
    assert_eq!(logical.tokens, ["next"]);
    assert_eq!(wire.tokens, ["old", "next"]);
    assert_eq!(reconnect.tokens, ["next"]);
    assert!(can_promote_seeded_clob_generation(
        connected_generation,
        clob_token_generation(&wire.tokens),
        books.has_all_seeded(&wire.tokens)
    ));
    assert!(!can_promote_seeded_clob_generation(
        clob_token_generation(&logical.tokens),
        connected_generation,
        true
    ));
    assert!(!commit_preseeded_clob_subscription(
        &mut logical,
        &wire,
        &mut reconnect,
        &books,
        &subscription(&["unseeded"]),
        true
    ));
    assert_eq!(logical.tokens, ["next"]);
    assert_eq!(reconnect.tokens, ["unseeded"]);
    // Wire traffic from the retired logical event must never reach the strategy.
    let mut batch = process_clob_frame(
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"old","price":"0.5","size":"10","side":"BUY","best_bid":"0.5","best_ask":"0.6"}],"timestamp":"2001"}"#,
        &mut books,
        &wire.tokens,
        now,
        2_001_000_000,
    );
    batch
        .events
        .retain(|event| should_forward_clob_event(event, &logical.tokens));
    assert!(batch.events.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn newer_subset_cancels_inflight_and_completed_stale_candidates() {
    for completed in [false, true] {
        let mut pending = Some((subscription(&["stale"]), true));
        let mut candidate = Some(tokio::spawn(async move {
            if !completed {
                std::future::pending::<()>().await;
            }
            Err("stale candidate".to_string())
        }));
        let abort = candidate.as_ref().unwrap().abort_handle();
        tokio::task::yield_now().await;
        cancel_clob_cutover(&mut pending, &mut candidate);
        tokio::task::yield_now().await;
        assert!(pending.is_none());
        assert!(candidate.is_none());
        assert!(abort.is_finished());
    }
}

#[test]
#[ignore = "focused BBO-changing frame benchmark; run with --test-threads=1"]
fn benchmark_clob_bbo_change() {
    let tokens = vec!["up".to_owned()];
    let mut books = ClobLocalBooks::default();
    let now = Instant::now();
    process_clob_frame(
        r#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.60","size":"11"}],"timestamp":"2000"}"#,
        &mut books,
        &tokens,
        now,
        2_000_000_000,
    );
    // Alternate real BBO changes; this cannot fall into the quantity-only fast path.
    let frames = [
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.60","size":"0","side":"SELL","best_bid":"0.40","best_ask":"0.70"},{"asset_id":"up","price":"0.70","size":"9","side":"SELL","best_bid":"0.40","best_ask":"0.70"}],"timestamp":"2001"}"#,
        r#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.70","size":"0","side":"SELL","best_bid":"0.40","best_ask":"0.60"},{"asset_id":"up","price":"0.60","size":"9","side":"SELL","best_bid":"0.40","best_ask":"0.60"}],"timestamp":"2001"}"#,
    ];
    let mut parser = ResidentClobParser::new();
    let mut input = frames[0].as_bytes().to_vec();
    let mut run = |i: usize| {
        input.copy_from_slice(frames[i % 2].as_bytes());
        let batch = process_clob_frame_in_place(
            &mut input,
            &mut parser,
            &mut books,
            &tokens,
            &tokens,
            now,
            2_001_000_000,
            &mut ClobFramePhaseTimings::default(),
        );
        assert_eq!(batch.wire.quantity_only_frames, 0);
        assert!(batch.diagnostics.is_empty());
        assert!(!batch.events.is_empty());
        std::hint::black_box(batch);
    };
    for i in 0..256 {
        run(i);
    }
    let n = 100_000;
    let mut samples = Vec::with_capacity(n);
    let (_, allocations, bytes) = clob_test_allocator::count(|| {
        for i in 0..n {
            let started = Instant::now();
            run(i);
            samples.push(started.elapsed().as_nanos() as u64);
        }
    });
    samples.sort_unstable();
    eprintln!("clob BBO change: boundary=resident_parse+apply+event_construction+batch_drop n={n} allocations={allocations} bytes={bytes} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_hwm=0 overflow=0 (same-thread benchmark)", samples[n/2-1], samples[n*99/100-1], samples[n*999/1000-1], samples[n-1]);
}
