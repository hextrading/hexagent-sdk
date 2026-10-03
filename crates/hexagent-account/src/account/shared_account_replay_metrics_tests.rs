use super::*;

#[test]
fn replay_metrics_do_not_wait_for_account_locks_and_remain_isolated() {
    let account = Arc::new(SharedAccount::new("replay-metrics"));
    let other = SharedAccount::new("other");
    let control = account.control_gate.write().unwrap();
    let state = account.state.lock().unwrap();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let caller = account.clone();
    let worker = std::thread::spawn(move || {
        caller.record_gap_replay_pages(3);
        caller.record_gap_replay_pages(0);
        caller.record_gap_replay_pages(7);
        tx.send(()).unwrap();
    });
    // Release guards even on failure so this regression cannot deadlock tests.
    let completed_while_locked = rx.recv_timeout(Duration::from_secs(1)).is_ok();
    drop(state);
    drop(control);
    worker.join().unwrap();
    assert!(
        completed_while_locked,
        "telemetry must not enter account locks"
    );
    let snapshot = account.monitoring_snapshot();
    assert_eq!(
        (
            snapshot.gap_replay_last_pages,
            snapshot.gap_replay_max_pages,
            snapshot.gap_replay_total_pages
        ),
        (7, 7, 10)
    );
    assert_eq!(other.monitoring_snapshot().gap_replay_total_pages, 0);
}

#[test]
fn replay_metrics_preserve_concurrent_totals_and_saturate() {
    let metrics = Arc::new(ReplayPageMetrics::default());
    let workers: Vec<_> = (0..4)
        .map(|_| {
            let metrics = metrics.clone();
            std::thread::spawn(move || {
                for _ in 0..1000 {
                    metrics.record(2);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(metrics.snapshot(), (2, 2, 8000));
    let saturated = ReplayPageMetrics::new(3, 8, u64::MAX - 1);
    saturated.record(4);
    assert_eq!(saturated.snapshot(), (4, 8, u64::MAX));
}

#[test]
fn replay_metrics_persist_with_cold_checkpoint_and_continue_after_restart() {
    let _guard = super::tests::persistence_test_guard();
    let path = std::env::temp_dir().join(format!(
        "replay-metrics-{}-{}.json",
        std::process::id(),
        wall_clock_ms()
    ));
    let account = Arc::new(SharedAccount::new_persistent("replay-metrics", &path).unwrap());
    account.register_instance("a", 1.0);
    // Live mode sends checkpoints through the cold owner projection. The
    // legacy unbound virtual-only checkpoint intentionally stores no counters.
    let _lifecycle = account.bind_account_lifecycle_owner().unwrap();
    account.record_gap_replay_pages(3);
    account.record_gap_replay_pages(7);
    account
        .record_sidecar_checkpoint(
            "a",
            DurableSidecarCheckpoint {
                generation: 1,
                expected_entries: 1,
                recovery_payload: "{}".into(),
            },
        )
        .unwrap();
    account.flush_persistence(Duration::from_secs(5)).unwrap();
    drop(_lifecycle);
    drop(account);
    let restored = SharedAccount::new_persistent("replay-metrics", &path).unwrap();
    assert_eq!(restored.replay_page_metrics.snapshot(), (7, 7, 10));
    restored.record_gap_replay_pages(4);
    assert_eq!(restored.monitoring_snapshot().gap_replay_total_pages, 14);
}

#[test]
#[ignore = "focused release counter-call benchmark, not live trading latency"]
fn replay_metrics_record_benchmark() {
    const N: usize = 4096;
    let account = SharedAccount::new("replay-metrics-bench");
    account.register_instance("a", 1.0);
    let mut before = Vec::with_capacity(N);
    let mut after = Vec::with_capacity(N);
    for _ in 0..N {
        let start = Instant::now();
        {
            // Original record_gap_replay_pages body, including guard publication.
            let mut state = account.lock_state();
            state.gap_replay_last_pages = 1;
            state.gap_replay_max_pages = state.gap_replay_max_pages.max(1);
            state.gap_replay_total_pages = state.gap_replay_total_pages.saturating_add(1);
        }
        before.push(start.elapsed().as_nanos());
        let start = Instant::now();
        account.record_gap_replay_pages(1);
        after.push(start.elapsed().as_nanos());
    }
    for (name, mut values) in [
        ("before_account_transaction", before),
        ("after_atomic_record", after),
    ] {
        values.sort_unstable();
        eprintln!("replay_metrics {name}: n={N} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0", (values[N/2-1]+values[N/2])/2, values[(N*99).div_ceil(100)-1], values[(N*999).div_ceil(1000)-1], values[N-1]);
    }
}
