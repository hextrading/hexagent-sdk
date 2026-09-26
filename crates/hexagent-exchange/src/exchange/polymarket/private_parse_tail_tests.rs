use super::*;

const FRAME: &str = r#"[{"event_type":"order","id":"0x0123456789abcdef","asset_id":"12345678901234567890","price":"0.42","original_size":"5","size_matched":"0","side":"SELL","type":"PLACEMENT","timestamp":"1790427024000"},{"event_type":"trade","id":"trade-id","asset_id":"12345678901234567890","price":"0.42","size":"5","side":"SELL","status":"MATCHED","maker_orders":[{"order_id":"0x0123456789abcdef","matched_amount":"5","price":"0.42"}],"timestamp":"1790427024001"}]"#;

#[test]
fn private_frame_preserves_payload_order_and_rejects_corruption() {
    for frame in [
        FRAME,
        r#"{"event_type":"trade","id":"\u6210交","size":5,"price":0.42,"maker_orders":[],"flag":null}"#,
        "[]",
    ] {
        let old: serde_json::Value =
            simd_json::serde::from_slice(&mut frame.as_bytes().to_vec()).unwrap();
        let expected = if old.is_array() {
            old.as_array().cloned().unwrap()
        } else {
            vec![old]
        };
        assert_eq!(parse_private_frame(frame).unwrap(), expected);
    }
    let events = parse_private_frame(FRAME).unwrap();
    assert_eq!(events[0]["event_type"], "order");
    assert_eq!(events[1]["event_type"], "trade");
    for invalid in ["{", "[] trailing", "[{\"id\":]", "{\"id\":\"\\uD800\"}"] {
        assert!(parse_private_frame(invalid).is_err(), "{invalid}");
    }
}

#[test]
#[ignore = "focused private parse allocation/tail comparison; release single test thread"]
fn benchmark_private_frame_parse_candidates() {
    const N: usize = 20000;
    for mode in ["legacy", "resident_buffers", "serde_borrowed_input"] {
        let mut buffers = simd_json::Buffers::new(16 * 1024);
        let mut input = Vec::with_capacity(16 * 1024);
        let mut run = || {
            let data: serde_json::Value = match mode {
                "legacy" => simd_json::serde::from_slice(&mut FRAME.as_bytes().to_vec()).unwrap(),
                "resident_buffers" => {
                    input.clear();
                    input.extend_from_slice(FRAME.as_bytes());
                    simd_json::serde::from_slice_with_buffers(&mut input, &mut buffers).unwrap()
                }
                _ => serde_json::from_str(FRAME).unwrap(),
            };
            let events = if mode == "legacy" {
                data.as_array().cloned().unwrap()
            } else {
                match data {
                    serde_json::Value::Array(events) => events,
                    _ => unreachable!(),
                }
            };
            std::hint::black_box(events);
        };
        for _ in 0..128 {
            run();
        }
        let (_, allocations, bytes) = super::super::market::clob_test_allocator::count(&mut run);
        let mut samples = Vec::with_capacity(N);
        for _ in 0..N {
            let start = std::time::Instant::now();
            run();
            samples.push(start.elapsed().as_nanos());
        }
        samples.sort_unstable();
        eprintln!("private_parse mode={mode} n={N} p50={} p99={} p999={} max={} unit=ns allocations={allocations} bytes={bytes} queue_depth=0 overflow=0 boundary=owned_json_parse_array_handoff_drop",samples[N/2],samples[N*99/100],samples[N*999/1000],samples[N-1]);
    }
}
