//! Portable benchmark also compiled against the unmodified c0f4e983 baseline.
use super::*;

#[test]
#[ignore = "focused publication policy benchmark; run explicitly in release"]
fn clob_continuous_publication_benchmark() {
    const N: usize = 1000;
    let tokens = vec!["up".to_string(), "down".to_string()];
    let specs = [CanonicalEventSpec { condition_id: "condition".into(), up_token: "up".into(), down_token: "down".into(), tick_size: 0.01 }];
    let seed = r#"[{"event_type":"book","asset_id":"up","bids":[{"price":"0.55","size":"10"}],"asks":[{"price":"0.56","size":"11"}],"timestamp":"2000"},{"event_type":"book","asset_id":"down","bids":[{"price":"0.44","size":"11"}],"asks":[{"price":"0.45","size":"10"}],"timestamp":"2000"}]"#;
    let frames: Vec<_> = (1..=9).map(|i| format!(r#"{{"event_type":"price_change","price_changes":[{{"asset_id":"up","price":"0.55{i}","size":"12","side":"BUY","best_bid":"0.55{i}","best_ask":"0.56"}}],"timestamp":"{}"}}"#,2000+i)).collect();
    let mut parser = ResidentClobParser::new();
    let mut buffer = Vec::with_capacity(4096);
    let mut cpu = Vec::with_capacity(N*9);
    let mut holds = Vec::with_capacity(N);
    for iteration in 0..N+100 {
        let mut books = ClobLocalBooks::new(&specs);
        let now = Instant::now();
        process_clob_frame(seed, &mut books, &tokens, now, 2_000_000_000);
        let mut first_publication = None;
        for (i,frame) in frames.iter().enumerate() {
            let offset = Duration::from_millis(1+i as u64*10);
            let start = std::time::Instant::now();
            buffer.clear();buffer.extend_from_slice(frame.as_bytes());
            let batch = process_clob_frame_in_place(&mut buffer, &mut parser, &mut books, &tokens, &tokens, now+offset, 2_001_000_000+i as u64*10_000_000, &mut ClobFramePhaseTimings::default());
            std::hint::black_box(&batch);
            let elapsed = start.elapsed().as_nanos() as u64;
            if iteration>=100 { cpu.push(elapsed); }
            if batch.events.iter().any(|e| matches!(e, MarketEvent::OrderBook(_))) {
                first_publication.get_or_insert(offset);
            }
        }
        if first_publication.is_none() {
            let deadline = books.next_deferred_deadline().expect("baseline must schedule publication");
            let batch = books.flush_deferred_due(deadline, 2_131_000_000, &tokens);
            assert!(batch.events.iter().any(|e| matches!(e, MarketEvent::OrderBook(_))));
            first_publication = Some(deadline.duration_since(now));
        }
        if iteration>=100 { holds.push((first_publication.unwrap()-Duration::from_millis(1)).as_nanos() as u64); }
    }
    for (boundary,mut values) in [("resident_frame_copy_parse_apply_wall",cpu),("virtual_first_valid_update_to_first_publication",holds)] {
        values.sort_unstable();let n=values.len();
        eprintln!("clob_no_wait boundary={boundary} n={n} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 schedule=9_frames_10ms_apart_no_sleep warmup_iterations=100 setup=excluded",values[n/2-1],values[n*99/100-1],values[n*999/1000-1],values[n-1]);
    }
}
