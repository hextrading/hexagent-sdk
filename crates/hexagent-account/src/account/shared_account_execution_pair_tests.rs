use super::*;

#[test]
fn historical_execution_pair_rebuilds_from_existing_audit_wal_after_restart() {
    let _guard = tests::persistence_test_guard();
    let path = std::env::temp_dir().join(format!(
        "hexagent-execution-pair-{}-{}.json",
        std::process::id(),
        wall_clock_ms()
    ));
    {
        let account = SharedAccount::new_persistent("pair-restart", &path).unwrap();
        account.register_instance("btc", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        account
            .register_token_interest("btc", "ended", "UP", "DOWN")
            .unwrap();
        account
            .retain_settled_event_audit("btc", "ended", &["UP".into(), "DOWN".into()])
            .unwrap();
        {
            let mut state = account.lock_state();
            state
                .instances
                .get_mut("btc")
                .unwrap()
                .token_interests
                .get_mut("ended")
                .unwrap()
                .retire_after_ms = Some(0);
        }
        assert!(account.token_interests().is_empty());
        account.flush_persistence(Duration::from_secs(2)).unwrap();
    }
    let restored = SharedAccount::new_persistent("pair-restart", &path).unwrap();
    assert!(
        restored.token_interests().is_empty(),
        "replay proof must not revive wallet query scope"
    );
    assert!(restored.private_execution_binary_pair("ended", "DOWN", "UP"));
    assert!(!restored.private_execution_binary_pair("other", "DOWN", "UP"));
    assert!(!restored.private_execution_binary_pair("ended", "DOWN", "FOREIGN"));
    assert!(
        !SharedAccount::new("foreign-account").private_execution_binary_pair("ended", "DOWN", "UP")
    );
    drop(restored);
    tests::remove_persistence_test_files(&path);
}

#[test]
fn historical_execution_pair_conflicts_and_nonbinary_scopes_fail_closed() {
    let account = SharedAccount::new("pair-conflict");
    account.register_instance("btc", 1.0);
    account.register_instance("sibling", 1.0);
    account
        .register_token_interest("btc", "same", "UP", "DOWN")
        .unwrap();
    // BTreeSet order differs from the instrument's up/down order.
    account
        .retain_settled_event_audit("btc", "same", &["DOWN".into(), "UP".into()])
        .unwrap();
    assert!(account.private_execution_binary_pair("same", "UP", "DOWN"));
    account
        .register_token_interest("sibling", "same", "UP", "FOREIGN")
        .unwrap();
    for right in ["DOWN", "FOREIGN"] {
        assert!(!account.private_execution_binary_pair("same", "UP", right));
    }
    account
        .register_token_interest("btc", "multi", "A", "B")
        .unwrap();
    account
        .retain_settled_event_audit("btc", "multi", &["A".into(), "B".into(), "C".into()])
        .unwrap();
    assert!(!account.private_execution_binary_pair("multi", "A", "B"));
}

#[test]
#[ignore = "focused cold immutable pair-publication before/after benchmark"]
fn benchmark_historical_execution_pair_publication() {
    use std::hint::black_box;
    let account = SharedAccount::new("pair-bench");
    account.register_instance("btc", 1.0);
    for index in 0..2 {
        account
            .register_token_interest(
                "btc",
                &format!("live-{index}"),
                &format!("U{index}"),
                &format!("D{index}"),
            )
            .unwrap();
    }
    for index in 0..5 {
        account
            .retain_settled_event_audit(
                "btc",
                &format!("ended-{index}"),
                &[format!("EU{index}"), format!("ED{index}")],
            )
            .unwrap();
    }
    let state = account.lock_state();
    let legacy = |state: &SharedAccountState| {
        let mut pairs = HashMap::new();
        for interest in state
            .instances
            .values()
            .flat_map(|instance| instance.token_interests.values())
        {
            let pair = (interest.up_token_id.clone(), interest.down_token_id.clone());
            pairs
                .entry(interest.condition_id.clone())
                .and_modify(|prior: &mut Option<(String, String)>| {
                    if prior.as_ref().is_none_or(|prior| {
                        prior != &pair && (prior.0 != pair.1 || prior.1 != pair.0)
                    }) {
                        *prior = None;
                    }
                })
                .or_insert(Some(pair));
        }
        pairs
    };
    for mode in ["before", "after"] {
        let mut samples = Vec::with_capacity(100_000);
        for _ in 0..100_000 {
            let start = std::time::Instant::now();
            let publication = if mode == "before" {
                legacy(black_box(&state))
            } else {
                published_binary_pairs(black_box(&state))
            };
            black_box(&publication);
            samples.push(start.elapsed().as_nanos());
            drop(publication);
        }
        samples.sort_unstable();
        println!("pair_publication {mode} n={} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=prebuilt_cold_state_to_constructed_map excludes=publication_swap,destruction,IO,queues", samples.len(), samples[50_000], samples[99_000], samples[99_900], samples[99_999]);
    }
}

#[test]
fn execution_pair_outlives_wallet_interest_until_all_audit_owners_release() {
    let account = SharedAccount::new("pair-lifetime");
    account.register_instance("btc", 1.0);
    account.register_instance("sibling", 1.0);
    account
        .apply_physical_snapshot(100.0, HashMap::new())
        .unwrap();
    account
        .register_token_interest("btc", "ended", "UP", "DOWN")
        .unwrap();
    let tokens = vec!["UP".into(), "DOWN".into()];
    for owner in ["btc", "sibling"] {
        account
            .retain_settled_event_audit(owner, "ended", &tokens)
            .unwrap();
    }
    account
        .record_settlement_and_retire(
            "btc",
            "ended",
            &HashMap::from([("UP".into(), 1.0), ("DOWN".into(), 0.0)]),
        )
        .unwrap();
    {
        let mut state = account.lock_state();
        state
            .instances
            .get_mut("btc")
            .unwrap()
            .token_interests
            .get_mut("ended")
            .unwrap()
            .retire_after_ms = Some(0);
    }
    assert!(account.token_interests().is_empty());
    account
        .register_token_interest("btc", "next", "NEXT-UP", "NEXT-DOWN")
        .unwrap();
    assert!(account.private_execution_binary_pair("ended", "DOWN", "UP"));
    account
        .release_settled_event_audit("btc", "ended", &tokens)
        .unwrap();
    assert!(account.private_execution_binary_pair("ended", "DOWN", "UP"));
    account
        .release_settled_event_audit("sibling", "ended", &tokens)
        .unwrap();
    // Empty references still protect unresolved lifecycle rows until the
    // existing owner certificates authorize the final GC transaction.
    account
        .register_token_interest("btc", "next", "NEXT-UP", "NEXT-DOWN")
        .unwrap();
    assert!(account.private_execution_binary_pair("ended", "DOWN", "UP"));
    let mut btc = account.register_settled_gc_owner("btc").unwrap();
    let mut sibling = account.register_settled_gc_owner("sibling").unwrap();
    account.finalize_ready_settled_audit_retirements();
    btc.poll_once(&account).unwrap();
    sibling.poll_once(&account).unwrap();
    account.finalize_ready_settled_audit_retirements();
    assert!(!account.private_execution_binary_pair("ended", "DOWN", "UP"));
}
