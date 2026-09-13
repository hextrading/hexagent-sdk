use super::*;

fn coordinator_fixture(commit: bool) -> (Arc<SharedAccount>, SettledGcOwnerMailbox) {
    let account = Arc::new(SharedAccount::new("coordinator-queue-test"));
    account.register_instance("owner", 1.0);
    let mut owner = account.register_settled_gc_owner("owner").unwrap();
    account
        .retain_settled_event_audit("owner", "condition", &["UP".to_string()])
        .unwrap();
    account
        .release_settled_event_audit("owner", "condition", &["UP".to_string()])
        .unwrap();
    if commit {
        assert!(account
            .finalize_ready_settled_audit_retirements()
            .is_empty());
        assert!(owner.poll_once(&account).unwrap().is_some());
    }
    (account, owner)
}

fn assert_coordinator_defers_busy_cold_owner(commit: bool, hold_state: bool) {
    let (account, mut owner) = coordinator_fixture(commit);
    // Hold either cold lock until the lifecycle worker has replied. A blocking
    // implementation fails, but release before join prevents a hung test.
    let control = (!hold_state).then(|| account.control_gate.write().unwrap());
    let state = hold_state.then(|| account.state.lock().unwrap());
    let worker_account = Arc::clone(&account);
    let (done_tx, done_rx) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        let ready = worker_account.finalize_ready_settled_audit_retirements();
        done_tx.send(ready).unwrap();
    });
    let while_busy = done_rx.recv_timeout(Duration::from_millis(100));
    drop(state);
    drop(control);
    worker.join().unwrap();
    assert!(
        while_busy
            .expect("GC parked the lifecycle worker behind the cold owner")
            .is_empty(),
        "busy GC must preserve the candidate/certificate without committing"
    );
    assert!(account.has_settled_gc_candidates());
    if !commit {
        assert!(account
            .finalize_ready_settled_audit_retirements()
            .is_empty());
        assert!(owner.poll_once(&account).unwrap().is_some());
    }
    assert_eq!(
        account.finalize_ready_settled_audit_retirements(),
        vec![HashSet::from(["UP".to_string()])]
    );
    assert!(!account.has_settled_gc_candidates());
    assert!(account
        .finalize_ready_settled_audit_retirements()
        .is_empty());
    assert_eq!(
        account
            .settled_gc_request_queue_overflows
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        account
            .settled_gc_completion_queue_overflows
            .load(Ordering::Relaxed),
        0
    );
}

#[test]
fn settled_gc_candidate_never_parks_private_owner_on_cold_locks() {
    for hold_state in [false, true] {
        assert_coordinator_defers_busy_cold_owner(false, hold_state);
    }
}

#[test]
fn settled_gc_commit_retains_certificate_while_cold_owner_is_busy() {
    for hold_state in [false, true] {
        assert_coordinator_defers_busy_cold_owner(true, hold_state);
    }
}

fn quantiles(samples: &mut [u64]) -> [u64; 4] {
    samples.sort_unstable();
    [500, 990, 999, 1000].map(|q| samples[(samples.len() * q).div_ceil(1000) - 1])
}

#[test]
#[ignore = "focused reproduction of a GC turn delaying the next private message"]
fn benchmark_gc_cold_contention_private_queue() {
    const N: usize = 1000;
    // Quanta calibrates on first use; keep startup out of both measurements.
    let _ = crate::latency::Instant::now();
    crate::latency::prepare_thread_stages(&[
        "polymarket.account.settled_gc_coordinator_control_busy",
        "polymarket.account.settled_gc_coordinator_state_busy",
    ]);
    for commit in [false, true] {
        let mut samples = Vec::with_capacity(N);
        let mut gc_samples = Vec::with_capacity(N);
        for _ in 0..N {
            let (account, _owner) = coordinator_fixture(commit);
            let (held_tx, held_rx) = crossbeam_channel::bounded(1);
            let (release_tx, release_rx) = crossbeam_channel::bounded(1);
            let cold_account = Arc::clone(&account);
            let cold = std::thread::spawn(move || {
                let _guard = cold_account.control_gate.write().unwrap();
                held_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                std::thread::sleep(Duration::from_millis(2));
            });
            held_rx.recv().unwrap();
            // Two messages share a single consumer just like GC and private
            // lifecycle work. Measure enqueue → dequeue of the second message.
            let (tx, rx) = crossbeam_channel::bounded(2);
            tx.try_send(None).unwrap();
            tx.try_send(Some(Instant::now())).unwrap();
            let queue_peak = rx.len();
            assert_eq!(queue_peak, 2);
            assert!(rx.recv().unwrap().is_none());
            release_tx.send(()).unwrap();
            let started = Instant::now();
            let _ = account.finalize_ready_settled_audit_retirements();
            gc_samples.push(started.elapsed().as_nanos() as u64);
            samples.push(rx.recv().unwrap().unwrap().elapsed().as_nanos() as u64);
            cold.join().unwrap();
        }
        let private = quantiles(&mut samples);
        let gc = quantiles(&mut gc_samples);
        eprintln!("gc_queue_probe phase={} n={N} cold_hold_ms=2 boundary=private_enqueue_to_dequeue_after_gc p50_ns={} p99_ns={} p999_ns={} max_ns={} gc_p50_ns={} gc_p99_ns={} gc_p999_ns={} gc_max_ns={} queue_peak=2 capacity=2 overflow=0",
            if commit {"commit"} else {"candidate"},private[0],private[1],private[2],private[3],gc[0],gc[1],gc[2],gc[3]);
    }
}
