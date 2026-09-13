use super::tests::{applied_trade_with_generation, settled_gc_benchmark_account};
use super::*;
use std::hint::black_box;

#[derive(Clone, Debug)]
struct Row {
    token: String,
    order: String,
}
impl gc_index::IndexedRow for Row {
    fn token(&self) -> &str {
        &self.token
    }
    fn order_key(&self) -> Option<&str> {
        Some(&self.order)
    }
}
fn row(token: &str, order: &str) -> Row {
    Row {
        token: token.into(),
        order: order.into(),
    }
}

#[test]
fn gc_index_replacement_replay_and_removal_preserve_identity() {
    let mut rows = TokenIndexedRows::default();
    rows.insert("trade".into(), row("A", "a"));
    rows.insert("trade".into(), row("A", "a"));
    assert_eq!(rows.rows_for_order("a").count(), 1);
    rows.insert("trade".into(), row("B", "b"));
    assert_eq!(rows.rows_for_order("a").count(), 0);
    assert!(!rows.has_tokens(&HashSet::from(["A".into()])));
    assert_eq!(rows.rows_for_order("b").next().unwrap().0, "trade");
    let restored: TokenIndexedRows<_> = rows.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(restored.rows_for_order("b").count(), 1);
    rows.remove("trade");
    assert!(rows.remove("trade").is_none());
    assert_eq!(rows.rows_for_order("b").count(), 0);
    assert!(!rows.has_tokens(&HashSet::from(["B".into()])));
    assert_eq!(restored.len(), 1);
}

#[test]
fn gc_cursor_rotates_past_protected_prefix_across_snapshot_replay() {
    let mut rows = TokenIndexedRows::default();
    for i in 0..400 {
        rows.insert(format!("{i:04}"), row("A", "a"));
    }
    rows.insert("unrelated".into(), row("B", "b"));
    let tokens = HashSet::from(["A".into()]);
    let mut seen = HashSet::new();
    for _ in 0..4 {
        let keys = rows.scan_keys(&tokens, 128);
        assert!(keys.len() <= 128);
        assert!(keys.iter().all(|key| key.as_ref() != "unrelated"));
        seen.extend(keys.iter().map(|key| key.to_string()));
        let restored = rows
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        rows.replace_rows(restored.into_iter());
    }
    assert_eq!(
        seen.len(),
        400,
        "cold replay must not reset a protected-prefix cursor"
    );
    for key in seen {
        rows.remove(&key);
    }
    rows.insert("0000".into(), row("A", "a"));
    assert_eq!(rows.scan_keys(&tokens, 128)[0].as_ref(), "0000");
    assert!(rows.scan_keys(&tokens, 0).is_empty());
}

#[test]
fn route_batch_preserves_old_readers_ownership_and_idempotence() {
    let routes = ShardedRouteMap::new();
    routes.insert("a".into(), "owner".into());
    routes.insert("b".into(), "sibling".into());
    let shard = ShardedRouteMap::shard_index("a");
    let old = routes.shards[shard].published.load_full();
    routes.apply_batch(
        "owner",
        &["a".into(), "b".into()],
        &[("c".into(), "owner".into())],
    );
    assert_eq!(old.get("a").unwrap().as_ref(), "owner");
    assert_eq!(routes.get("a"), None);
    assert_eq!(routes.get("b").as_deref(), Some("sibling"));
    assert_eq!(routes.get("c").as_deref(), Some("owner"));
    let before: Vec<_> = routes
        .shards
        .iter()
        .map(|s| s.published.load_full())
        .collect();
    routes.apply_batch(
        "owner",
        &["a".into(), "b".into()],
        &[("c".into(), "owner".into())],
    );
    assert!(before
        .iter()
        .zip(routes.shards.iter())
        .all(|(old, shard)| Arc::ptr_eq(old, &shard.published.load_full())));
}

#[test]
fn route_snapshot_combines_insert_prune_and_respects_private_epoch() {
    let routes = ShardedRouteMap::new();
    routes.insert("old".into(), "owner".into());
    routes.insert("new-private".into(), "owner".into());
    routes.insert("other".into(), "sibling".into());
    let epoch = AtomicU64::new(2);
    let keys = HashSet::from(["desired".into()]);
    routes.publish_owners(
        &[RouteOwnerSnapshot {
            owner: "owner",
            keys: &keys,
            epoch: Some((&epoch, 1)),
        }],
        None,
    );
    assert_eq!(routes.get("new-private").as_deref(), Some("owner"));
    routes.publish_owners(
        &[RouteOwnerSnapshot {
            owner: "owner",
            keys: &keys,
            epoch: Some((&epoch, 2)),
        }],
        None,
    );
    assert_eq!(routes.get("old"), None);
    assert_eq!(routes.get("new-private"), None);
    assert_eq!(routes.get("other").as_deref(), Some("sibling"));
    let before: Vec<_> = routes
        .shards
        .iter()
        .map(|s| s.published.load_full())
        .collect();
    routes.publish_owners(
        &[RouteOwnerSnapshot {
            owner: "owner",
            keys: &keys,
            epoch: None,
        }],
        None,
    );
    assert!(before
        .iter()
        .zip(routes.shards.iter())
        .all(|(old, shard)| Arc::ptr_eq(old, &shard.published.load_full())));
}

#[test]
fn settled_prune_delta_is_immutable_and_replays_exact_state() {
    let account = settled_gc_benchmark_account();
    let mut state = account.state.lock().unwrap();
    let before = serde_json::to_value(&*state).unwrap();
    let outcome = prune_terminal_history_locked(
        &mut state,
        Some("owner"),
        &HashSet::from(["SETTLED".into()]),
    );
    let expected = serde_json::to_value(&*state).unwrap();
    let delta = SettledPrunePersistenceDelta::capture(&state, &[outcome], &[]);
    state.orders.clear();
    state.trades.clear();
    state.compacted_economic_effects = Default::default();
    drop(state);
    let changes = delta.materialize().unwrap();
    let mut actual = before;
    for _ in 0..2 {
        // duplicate replay is idempotent
        apply_persistence_wal_changes_transactional(&mut actual, &changes).unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn settled_prune_is_a_persistence_coalescing_barrier() {
    let account = settled_gc_benchmark_account();
    let state = account.state.lock().unwrap();
    let delta = SettledPrunePersistenceDelta::capture(&state, &[], &[]);
    let anchor = |generation, value| PersistenceJob {
        generation,
        payload: PersistenceJobPayload::UnresolvedTradeMatchTime {
            trade_key: "same".into(),
            match_time_secs: Some(value),
        },
    };
    let jobs = coalesce_persistence_jobs(vec![
        anchor(1, 1),
        PersistenceJob {
            generation: 2,
            payload: PersistenceJobPayload::SettledPrune(delta),
        },
        anchor(3, 3),
        anchor(4, 4),
    ]);
    assert_eq!(
        jobs.iter().map(|j| j.generation).collect::<Vec<_>>(),
        vec![1, 2, 4]
    );
}

fn report(label: &str, samples: &mut [u64]) {
    samples.sort_unstable();
    let q = |p: usize| samples[(samples.len() * p).div_ceil(1000) - 1];
    eprintln!("batch_benchmark boundary={label} n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0", samples.len(), q(500), q(990), q(999), q(1000));
}

#[test]
#[ignore = "focused before/after cold-owner and private-index benchmark"]
fn benchmark_gc_index_routes_and_persistence_capture() {
    const N: usize = 1000;
    let mut indexed = TokenIndexedRows::default();
    let mut raw = HashMap::new();
    for i in 0..38_000 {
        let key = format!("trade-{i:06}");
        let value = row(if i < 100 { "SETTLED" } else { "OTHER" }, "coid");
        indexed.insert(key.clone(), value.clone());
        raw.insert(key, value);
    }
    let tokens = HashSet::from(["SETTLED".into()]);
    let mut scan = Vec::new();
    let mut lookup = Vec::new();
    for _ in 0..N {
        let t = Instant::now();
        black_box(
            raw.iter()
                .filter(|(_, v)| tokens.contains(&v.token))
                .count(),
        );
        scan.push(t.elapsed().as_nanos() as u64);
        let t = Instant::now();
        black_box(indexed.scan_keys(&tokens, 128));
        lookup.push(t.elapsed().as_nanos() as u64);
    }
    report("history_scan_38000", &mut scan);
    report("indexed_candidates_128", &mut lookup);
    let mut plain_insert = Vec::new();
    let mut index_insert = Vec::new();
    for i in 0..N {
        let key = format!("new-{i}");
        let value = row("OTHER", &key);
        let (k, v) = (key.clone(), value.clone());
        let t = Instant::now();
        raw.insert(k, v);
        plain_insert.push(t.elapsed().as_nanos() as u64);
        let t = Instant::now();
        indexed.insert(key, value);
        index_insert.push(t.elapsed().as_nanos() as u64);
    }
    report("private_plain_insert", &mut plain_insert);
    report("private_indexed_insert", &mut index_insert);
    let routes = ShardedRouteMap::new();
    let keys = (0..2000)
        .map(|i| format!("route-{i}"))
        .collect::<HashSet<_>>();
    let mut old = Vec::new();
    let mut batch = Vec::new();
    for _ in 0..N {
        let t = Instant::now();
        for k in &keys {
            routes.insert(k.clone(), "owner".into());
        }
        routes.retain_owner_keys("owner", &keys);
        old.push(t.elapsed().as_nanos() as u64);
        let t = Instant::now();
        routes.publish_owners(
            &[RouteOwnerSnapshot {
                owner: "owner",
                keys: &keys,
                epoch: None,
            }],
            None,
        );
        batch.push(t.elapsed().as_nanos() as u64);
    }
    report("route_2000_per_key_then_retain", &mut old);
    report("route_2000_shard_batch", &mut batch);
    let account = settled_gc_benchmark_account();
    let mut state = account.state.lock().unwrap();
    let mut outcome = prune_terminal_history_locked(&mut state, Some("owner"), &tokens);
    outcome.orders.truncate(8);
    outcome.trades.truncate(8);
    let outcomes = [outcome];
    let mut serde = Vec::new();
    let mut capture = Vec::new();
    for _ in 0..N {
        let t = Instant::now();
        black_box(legacy_prune_changes(&state, &outcomes, &[]));
        serde.push(t.elapsed().as_nanos() as u64);
        let t = Instant::now();
        black_box(SettledPrunePersistenceDelta::capture(
            &state,
            &outcomes,
            &[],
        ));
        capture.push(t.elapsed().as_nanos() as u64);
    }
    report("gc_8_orders_8_trades_inline_serde", &mut serde);
    report("gc_8_orders_8_trades_typed_capture", &mut capture);
    black_box(applied_trade_with_generation(1));
}

fn legacy_prune_changes(
    state: &SharedAccountState,
    outcomes: &[SettledPruneOutcome],
    retired_conditions: &[String],
) -> Result<Vec<PersistenceWalChange>, String> {
    (|| -> Result<Vec<PersistenceWalChange>, String> {
        let mut changes = Vec::new();
        for outcome in outcomes {
            for (coid, order_id) in &outcome.orders {
                persistence_wal_map_entry(&mut changes, "orders", coid, state.orders.get(coid))?;
                let normalized = normalize_order_id(order_id);
                persistence_wal_map_entry(
                    &mut changes,
                    "oid_to_coid",
                    &normalized,
                    state.oid_to_coid.get(&normalized),
                )?;
                persistence_wal_set_membership(
                    &mut changes,
                    "recovery_pending_orders",
                    coid,
                    state.recovery_pending_orders.contains(coid),
                )?;
                persistence_wal_set_membership(
                    &mut changes,
                    "startup_query_repair_orders",
                    coid,
                    state.startup_query_repair_orders.contains(coid),
                )?;
                persistence_wal_set_membership(
                    &mut changes,
                    "routine_cancel_audits",
                    coid,
                    state.routine_cancel_audits.contains(coid),
                )?;
            }
            for trade_key in &outcome.trades {
                persistence_wal_map_entry(
                    &mut changes,
                    "trades",
                    trade_key,
                    state.trades.get(trade_key),
                )?;
                persistence_wal_map_entry(
                    &mut changes,
                    "retired_trade_ownership_tombstones",
                    trade_key,
                    state.retired_trade_ownership_tombstones.get(trade_key),
                )?;
                persistence_wal_set_membership(
                    &mut changes,
                    "fee_attribution_pending",
                    trade_key,
                    state.fee_attribution_pending.contains(trade_key),
                )?;
            }
            for trade_key in &outcome.expired_tombstones {
                persistence_wal_map_entry::<RetiredTradeOwnershipTombstone>(
                    &mut changes,
                    "retired_trade_ownership_tombstones",
                    trade_key,
                    None,
                )?;
            }
            for token in &outcome.fee_tokens {
                persistence_wal_map_entry(
                    &mut changes,
                    "token_fee_configs",
                    token,
                    state.token_fee_configs.get(token),
                )?;
            }
        }
        if outcomes.iter().any(|outcome| !outcome.trades.is_empty()) {
            persistence_wal_set(
                &mut changes,
                ["compacted_economic_effects".to_string()],
                &state.compacted_economic_effects,
            )?;
        }
        for condition_id in retired_conditions {
            persistence_wal_map_entry(
                &mut changes,
                "settled_audit_references",
                condition_id,
                state.settled_audit_references.get(condition_id),
            )?;
        }
        Ok(changes)
    })()
}

#[test]
fn gc_coordinator_continues_incomplete_sweep_then_blocks_protected_rows() {
    let account = SharedAccount::new("scan-continuation");
    account.register_instance("owner", 1.0);
    let owner = account.virtual_account("owner").unwrap();
    let lifecycle = account.lifecycle_mut(&owner);
    let mut state = account.state.lock().unwrap();
    // First two batches are completely protected; the last row is eligible.
    for i in 0..301 {
        let mut trade = applied_trade_with_generation(i + 1);
        trade.ownership.instance_id = "owner".into();
        trade.ownership.token_id = "SETTLED".into();
        trade.ownership.trade_key = format!("trade-{i:04}");
        trade.ownership.status = if i == 300 { "CONFIRMED" } else { "MATCHED" }.into();
        trade.failed = false;
        let key = trade.ownership.trade_key.clone();
        lifecycle.trades.insert(key.clone(), trade.clone());
        state.trades.insert(key, trade);
    }
    drop(state);
    let tokens = HashSet::from(["SETTLED".into()]);
    super::tests::install_test_settled_gc_candidate(&account, "condition", &tokens);
    let mut mailbox = account.register_settled_gc_owner("owner").unwrap();
    for i in 0..3 {
        assert!(account
            .finalize_ready_settled_audit_retirements()
            .is_empty());
        let cert = mailbox
            .poll_once(&account)
            .unwrap()
            .expect("incomplete sweep must redispatch without new private activity");
        assert_eq!(cert.retired_trades, if i == 2 { 1 } else { 0 });
        assert_eq!(cert.scan_incomplete, i < 2);
    }
    assert!(!account.lifecycle(&owner).trades.contains_key("trade-0300"));
    // Remaining rows must never be removed; after a complete no-progress sweep
    // the coordinator sleeps until private activity, rather than busy-looping.
    for _ in 0..4 {
        assert!(account
            .finalize_ready_settled_audit_retirements()
            .is_empty());
        if mailbox.poll_once(&account).unwrap().is_none() {
            break;
        }
    }
    assert!(account
        .settled_gc_coordinator
        .lock()
        .unwrap()
        .inflight
        .is_none());
    assert_eq!(account.lifecycle(&owner).trades.len(), 300);
}
