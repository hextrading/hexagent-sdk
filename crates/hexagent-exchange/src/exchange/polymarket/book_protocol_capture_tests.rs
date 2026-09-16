use super::*;
use crate::recorder::{
    BookProtocolLane, BookProtocolOwnerScope, BookProtocolRecord, MarketRecorder,
};

const BOOK: &[u8] = br#"{"event_type":"book","asset_id":"up","bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.42","size":"11"}],"timestamp":"1789307400123","hash":"venue-book-hash"}"#;
const DELTA: &[u8] = br#"{"event_type":"price_change","price_changes":[{"asset_id":"up","price":"0.40","size":"9","side":"BUY","hash":"venue-delta-hash"}],"timestamp":"1789307400124","sequence":"12","previous_sequence":11}"#;

fn route() -> BookProtocolRoute {
    BookProtocolRoute::new("up", "cid-1", Some(1789307400)).unwrap()
}
fn read_records(dir: &std::path::Path, capture: u64) -> Vec<BookProtocolRecord> {
    std::fs::read_to_string(dir.join(format!("book_protocol/{capture}.records.jsonl")))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn read_manifest(dir: &std::path::Path, capture: u64) -> serde_json::Value {
    serde_json::from_slice(
        &std::fs::read(dir.join(format!("book_protocol/{capture}.manifest.json"))).unwrap(),
    )
    .unwrap()
}

#[test]
fn actual_parser_to_lane_to_recorder_preserves_raw_and_owner_scope() {
    let dir = tempfile::tempdir().unwrap();
    let lane = BookProtocolLane::new(11);
    let mut consumer = lane.consumer(dir.path()).unwrap();
    assert!(
        lane.consumer(dir.path()).is_err(),
        "only one recorder consumer"
    );
    let mut recorder = MarketRecorder::new(dir.path().into()).unwrap();
    let mut session = lane.sink().session(&[route()], 1789307400130000000);
    let mut parser = ResidentClobParser::new();
    let mut books = ClobLocalBooks::default();
    let mut control_parser = ResidentClobParser::new();
    let mut control_books = ClobLocalBooks::default();
    let tokens = vec!["up".to_string()];
    for raw in [BOOK, DELTA, DELTA] {
        let receive_ns = 1789307400200000000;
        session.capture_raw(raw, receive_ns, "active");
        let mut input = raw.to_vec();
        let mut phases = ClobFramePhaseTimings::default();
        let mut control_input = raw.to_vec();
        // Compare normalized outputs: evidence must not alter local book rules.
        let control = process_clob_frame_in_place(
            &mut control_input,
            &mut control_parser,
            &mut control_books,
            &tokens,
            &tokens,
            Instant::now(),
            receive_ns + 500,
            &mut phases,
        );
        let observed = process_clob_frame_in_place_observed(
            &mut input,
            &mut parser,
            &mut books,
            &tokens,
            &tokens,
            Instant::now(),
            receive_ns + 500,
            &mut phases,
            Some(&mut session),
        );
        assert_eq!(
            serde_json::to_value(control.events).unwrap(),
            serde_json::to_value(observed.events).unwrap()
        );
    }
    drop(session);
    consumer.finish(&mut recorder).unwrap();
    let rows = read_records(dir.path(), 11);
    assert_eq!(rows.len(), 5); // reconnect, snapshot, two non-deduplicated delta observations, close gap
    let snapshot = &rows[1];
    assert_eq!(snapshot.owner_scope, BookProtocolOwnerScope::PublicFeed);
    assert_eq!(snapshot.iid, "feed:polymarket");
    assert!(snapshot.validate_strategy_owner("feed:polymarket").is_err());
    assert!(snapshot.validate_strategy_owner("btc01").is_err());
    assert!(
        crate::exchange::sim_v2::BookContinuityReplay::from_reader(
            std::io::Cursor::new(serde_json::to_vec(snapshot).unwrap()),
            "btc01"
        )
        .is_err(),
        "public feed observations cannot be consumed as instance continuity"
    );
    assert_eq!(snapshot.venue_sequence, None);
    assert_eq!(snapshot.venue_book_hash.as_deref(), Some("venue-book-hash"));
    assert_eq!(snapshot.raw_exchange_timestamp, Some(1789307400123));
    assert_eq!(snapshot.exchange_timestamp_ns, Some(1789307400123000000));
    assert_eq!(snapshot.local_timestamp_ns, 1789307400200000000);
    assert_eq!(snapshot.event_epoch, 1789307400);
    assert_eq!(snapshot.event_id.as_deref(), Some("cid-1"));
    assert_eq!(rows[2].venue_sequence, Some(12));
    assert_eq!(rows[2].venue_previous_sequence, Some(11));
    assert_ne!(rows[2].recorder_sequence, rows[3].recorder_sequence);
    assert_eq!(rows[2].venue_sequence, rows[3].venue_sequence);
    let raw: Vec<serde_json::Value> =
        std::fs::read_to_string(dir.path().join("book_protocol/11.frames.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    assert_eq!(raw[0]["payload_utf8"], std::str::from_utf8(BOOK).unwrap());
    assert_eq!(raw[1]["payload_utf8"], std::str::from_utf8(DELTA).unwrap());
    assert_eq!(raw[2]["frame_sequence"], 3);
    assert_eq!(read_manifest(dir.path(), 11)["archive_complete"], true);
    assert_eq!(
        read_manifest(dir.path(), 11)["continuity_claim"],
        "none_raw_observations_only"
    );
}

#[test]
fn protocol_missing_fields_heartbeat_and_unattributed_event_remain_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let lane = BookProtocolLane::new(12);
    let mut consumer = lane.consumer(dir.path()).unwrap();
    let mut recorder = MarketRecorder::new(dir.path().into()).unwrap();
    let mut session = lane.sink().session(
        &[BookProtocolRoute::new("up", "cid-static", None).unwrap()],
        1,
    );
    let raw = br#"{"event_type":"book","asset_id":"up","bids":[],"asks":[]}"#;
    let mut parser = ResidentClobParser::new();
    let mut input = raw.to_vec();
    session.capture_raw(raw, 2, "active");
    parser
        .parse(&mut input, |value| {
            observe_clob_protocol_value(value, &mut session).unwrap()
        })
        .unwrap();
    session.capture_raw(b"PONG", 3, "standby");
    drop(session);
    consumer.finish(&mut recorder).unwrap();
    let rows = read_records(dir.path(), 12);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[1].exchange_timestamp_ns, None);
    assert_eq!(rows[1].venue_sequence, None);
    assert_eq!(rows[1].event_epoch, 0);
    assert_eq!(rows[1].venue_book_hash, None);
    assert!(!rows
        .iter()
        .any(|r| r.kind == BookProtocolKind::SequenceHeartbeat));
    assert_eq!(
        protocol_event_epoch("btc-updown-5m-1789307400"),
        Some(1789307400)
    );
    assert_eq!(protocol_event_epoch("static-election-market"), None);
}

#[test]
fn protocol_reconnect_session_isolation_and_conflicting_routes_fail_closed() {
    let dir = tempfile::tempdir().unwrap();
    let lane = BookProtocolLane::new(13);
    let mut consumer = lane.consumer(dir.path()).unwrap();
    let mut recorder = MarketRecorder::new(dir.path().into()).unwrap();
    let mut a = lane.sink().session(&[route()], 1);
    a.gap("socket_read_error", 2);
    drop(a);
    let b = lane.sink().session(&[route()], 3);
    drop(b);
    consumer.finish(&mut recorder).unwrap();
    let rows = read_records(dir.path(), 13);
    assert_eq!(
        rows.iter()
            .filter(|r| r.kind == BookProtocolKind::Reconnect)
            .count(),
        2
    );
    assert_ne!(rows[0].session_id, rows[3].session_id);
    assert!(rows
        .iter()
        .any(|r| r.wire_message_type == "socket_read_error"));
    let dir2 = tempfile::tempdir().unwrap();
    let lane2 = BookProtocolLane::new(14);
    let mut consumer2 = lane2.consumer(dir2.path()).unwrap();
    let mut recorder2 = MarketRecorder::new(dir2.path().into()).unwrap();
    let conflict = BookProtocolRoute::new("up", "other-cid", Some(1789307700)).unwrap();
    drop(lane2.sink().session(&[route(), conflict], 1));
    consumer2.finish(&mut recorder2).unwrap();
    assert_eq!(read_manifest(dir2.path(), 14)["archive_complete"], false);
    assert_eq!(read_manifest(dir2.path(), 14)["invalid"], 1);
}

#[test]
fn protocol_raw_pool_and_metadata_overflow_are_sticky_incomplete() {
    for raw_overflow in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let capture = if raw_overflow { 15 } else { 16 };
        let lane = BookProtocolLane::new(capture);
        let mut consumer = lane.consumer(dir.path()).unwrap();
        let mut recorder = MarketRecorder::new(dir.path().into()).unwrap();
        let mut session = lane.sink().session(&[route()], 1);
        if raw_overflow {
            for _ in 0..65 {
                session.capture_raw(BOOK, 2, "active");
            }
        } else {
            for _ in 0..8200 {
                session.record(
                    "up",
                    BookProtocolKind::Delta,
                    "price_change",
                    None,
                    None,
                    None,
                    None,
                    2,
                    None,
                );
            }
        }
        drop(session);
        consumer.finish(&mut recorder).unwrap();
        let manifest = read_manifest(dir.path(), capture);
        assert_eq!(manifest["archive_complete"], false);
        assert!(manifest["overflow"].as_u64().unwrap() > 0);
        assert_eq!(manifest["queued"], 0);
    }
}

#[test]
fn protocol_malformed_input_records_gap_and_incomplete_without_fabricating_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let lane = BookProtocolLane::new(17);
    let mut consumer = lane.consumer(dir.path()).unwrap();
    let mut recorder = MarketRecorder::new(dir.path().into()).unwrap();
    let mut session = lane.sink().session(&[route()], 1);
    let mut parser = ResidentClobParser::new();
    let raw = br#"{"event_type":"book","asset_id":"up","timestamp":"bad"}"#;
    session.capture_raw(raw, 2, "active");
    let mut input = raw.to_vec();
    process_clob_frame_in_place_observed(
        &mut input,
        &mut parser,
        &mut ClobLocalBooks::default(),
        &[],
        &[],
        Instant::now(),
        2,
        &mut ClobFramePhaseTimings::default(),
        Some(&mut session),
    );
    drop(session);
    consumer.finish(&mut recorder).unwrap();
    assert_eq!(read_manifest(dir.path(), 17)["archive_complete"], false);
    assert!(read_records(dir.path(), 17)
        .iter()
        .any(|r| r.kind == BookProtocolKind::Gap));
}

#[test]
fn protocol_oversize_records_gap_without_growing_pool() {
    let dir = tempfile::tempdir().unwrap();
    let lane = BookProtocolLane::new(19);
    let mut consumer = lane.consumer(dir.path()).unwrap();
    let mut recorder = MarketRecorder::new(dir.path().into()).unwrap();
    let mut session = lane.sink().session(&[route()], 1);
    session.capture_raw(&vec![b'x'; 128 * 1024 + 1], 2, "active");
    drop(session);
    consumer.finish(&mut recorder).unwrap();
    let manifest = read_manifest(dir.path(), 19);
    assert_eq!(manifest["archive_complete"], false);
    assert_eq!(manifest["oversize_frames"], 1);
    assert_eq!(manifest["raw_pool_high_water"], 0);
    assert!(read_records(dir.path(), 19)
        .iter()
        .any(|r| r.kind == BookProtocolKind::Gap
            && r.wire_message_type == "raw_frame_capacity_exceeded"));
}

#[test]
#[ignore = "100k focused raw copy + resident tape decode + fixed evidence enqueue benchmark; no disk/network/strategy"]
fn benchmark_book_protocol_capture_100k() {
    for (label, frame) in [("book", BOOK), ("delta", DELTA)] {
        for enabled in [false, true] {
            let lane = BookProtocolLane::new(18);
            let mut session = lane.sink().session(&[route()], 1);
            let mut parser = ResidentClobParser::new();
            let mut input = frame.to_vec();
            let mut samples = Vec::with_capacity(100_000);
            lane.recycle_test_messages();
            let (_, allocations, allocated_bytes) = clob_test_allocator::count(|| {
                for _ in 0..100_000 {
                    input.copy_from_slice(frame);
                    let start = Instant::now();
                    if enabled {
                        session.capture_raw(frame, 1789307400200000000, "active");
                    }
                    parser
                        .parse(&mut input, |value| {
                            std::hint::black_box(decode_clob_tape_value(value).unwrap());
                            if enabled {
                                observe_clob_protocol_value(value, &mut session).unwrap();
                            }
                        })
                        .unwrap();
                    samples.push(start.elapsed().as_nanos() as u64);
                    // Recycling is outside the measured producer boundary. Real
                    // recorder persistence is separately exercised by roundtrip.
                    lane.recycle_test_messages();
                }
            });
            assert_eq!((allocations, allocated_bytes), (0, 0));
            samples.sort_unstable();
            let (high_water, overflow, queued, raw_pool_high_water, raw_pool_overflow) =
                lane.test_counters();
            assert_eq!((overflow, queued), (0, 0));
            eprintln!("book_protocol_capture label={label} framesz={} enabled={enabled} n=100000 median_ns={} p99_ns={} p999_ns={} max_ns={} high_water={high_water} capacity=8192 raw_pool=64 raw_pool_high_water={raw_pool_high_water} raw_pool_overflow={raw_pool_overflow} raw_frame_capacity=131072 overflow={overflow} queued={queued} allocations={allocations} allocated_bytes={allocated_bytes} boundary=raw_copy+resident_decode+metadata_enqueue excludes=recycle,disk,network,book_apply,strategy", frame.len(), samples[50_000], samples[99_000], samples[99_900], samples[99_999]);
        }
    }
}
