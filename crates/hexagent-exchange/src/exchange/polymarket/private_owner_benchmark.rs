//! Common before/after libtest: no socket, credentials or cold account apply.
use super::*;

fn cpu_ns() -> u64 {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    unsafe {
        let mut t: libc::timespec = std::mem::zeroed();
        assert_eq!(
            libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut t),
            0
        );
        return t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64;
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    panic!("benchmark requires actual thread CPU clock");
}

#[test]
#[ignore = "offline owner routing/dispatch CPU and wall A/B; release, serial"]
fn private_owner_timing_benchmark() {
    hexagent_runtime::latency::prepare_polymarket_private_stages();
    let n: usize = std::env::var("HEXBOT_INVENTORY_BENCH_EVENTS")
        .map(|x| x.parse().unwrap())
        .unwrap_or(10_000);
    assert!(n > 0);
    let shared = super::tests::test_shared();
    shared.account_state.register_instance("owner-1", 1.0);
    shared
        .account_state
        .apply_physical_snapshot(100., HashMap::new())
        .unwrap();
    shared
        .account_state
        .reserve_order(
            "owner-1",
            "owner-1-1",
            "0xabc1",
            "TOKEN",
            Side::Buy,
            5.,
            0.5,
            0,
        )
        .unwrap();
    shared.register_order_id("owner-1-1", "0xabc1", "TOKEN");
    shared.flush_execution_state_for_test();
    for kind in ["order", "trade"] {
        let payload = if kind == "order" {
            serde_json::json!({
                "event_type":"order","type":"PLACEMENT","order_id":"0xabc1","asset_id":"TOKEN",
                "side":"BUY","price":"0.5","original_size":"5","size_matched":"0"
            })
        } else {
            serde_json::json!({
                "event_type":"trade","id":"owner-benchmark","status":"MATCHED","asset_id":"TOKEN","market":"test-condition",
                "side":"BUY","size":"2","price":"0.5","taker_order_id":"0xabc1",
                "maker_orders":[{"order_id":"other","asset_id":"TOKEN","side":"SELL","matched_amount":"2","price":"0.5"}]
            })
        };
        let receipt = ReceiptSequencer::new().received().parsed();
        let event = PrivateEventDelta::classify_with_timing(
            payload,
            LifecycleTiming {
                private_ws_received_ns: receipt.ws_received_mono_ns,
                private_json_parsed_ns: receipt.parsed_mono_ns,
                receipt,
                ..Default::default()
            },
        )
        .unwrap();
        let mut dedupe = PrivateRouteDedupe::new();
        // Production initializes the cache before owner readiness.
        dedupe.execution_cache = Some(
            PrivateExecutionCache::new(shared.account_state.private_execution_seed()).unwrap(),
        );
        let (tx, rx) = crossbeam_channel::bounded(1);
        let mut wall = Vec::with_capacity(n);
        let mut cpu = Vec::with_capacity(n);
        let mut started = Vec::with_capacity(n);
        let mut timings = Vec::with_capacity(n);
        let mut high_water = 0;
        for index in 0..n + 256 {
            // Fresh route admission each trial; SDK execution cache stays warm.
            // This measures normal delivery cost, not replay-suppressed traffic.
            dedupe.ranks.clear();
            dedupe.order.clear();
            let input = [event.clone()];
            let start_wall = crate::types::monotonic_now_ns();
            let start_cpu = cpu_ns();
            let result = route_private_batch(&shared, &tx, input, None, &mut dedupe, None);
            let done_cpu = cpu_ns();
            let done_wall = crate::types::monotonic_now_ns();
            result.unwrap();
            high_water = high_water.max(rx.len());
            let delivered = rx.recv().unwrap();
            assert_eq!(delivered.owner, 0);
            assert_eq!(
                delivered.timing.receipt.message_sequence,
                receipt.message_sequence
            );
            if index >= 256 {
                wall.push(done_wall - start_wall);
                cpu.push(done_cpu - start_cpu);
                started.push(start_wall);
                timings.push(delivered.timing);
            }
        }
        assert!(rx.is_empty());
        let extra = serde_json::json!({"kind":kind,"queue_depth":0,"queue_high_water":high_water,"overflow":0,
            "allocator":if cfg!(feature="offline-private-owner-mimalloc") {"mimalloc"}else{"System"}});
        let record = serde_json::json!({"schema":3,"sample_order":"event","case":format!("private_owner_{kind}"),
            "boundary":"private route batch entry through owned dispatch, including route allocation; excludes input, receive, cold apply and JSON",
            "extra":extra,"started_ns":started,"wall_ns":wall,"cpu_ns":cpu,"timings":timings});
        if let Ok(path) = std::env::var("HEXBOT_INVENTORY_BENCH_SAMPLES") {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            serde_json::to_writer(&mut file, &record).unwrap();
            writeln!(file).unwrap();
        }
        println!(
            "private_owner_benchmark kind={kind} n={n} queue_high_water={high_water} overflow=0"
        );
    }
}
