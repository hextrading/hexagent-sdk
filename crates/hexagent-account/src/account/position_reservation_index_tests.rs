use super::*;

fn assert_equivalent(indexed: &PositionManager, scan: &PositionManager) {
    assert_eq!(indexed.pending_orders(), scan.pending_orders());
    for (a, b) in [
        (indexed.locked_buy_cost(), scan.locked_buy_cost()),
        (indexed.available_cash(), scan.available_cash()),
        (indexed.locked_sell_qty("UP"), scan.locked_sell_qty("UP")),
        (indexed.locked_sell_qty("DN"), scan.locked_sell_qty("DN")),
        (
            indexed.available_inventory("UP"),
            scan.available_inventory("UP"),
        ),
    ] {
        assert!((a - b).abs() < 1e-8, "indexed={a} scan={b}");
    }
}

#[test]
fn reservation_index_matches_canonical_scan_across_replay_reversal_and_restore() {
    let mut scan =
        PositionManager::with_initial_quantities(HashMap::from([("UP".into(), 1000.0)]), 1000.0);
    scan.register_pending_order_with_cash_fee("before", "UP", Side::Buy, 0.4, 7.0, 0.01);
    let mut indexed = PositionManager::from_snapshot(scan.snapshot()).unwrap();
    indexed.enable_incremental_queries();
    // Same ID changes side and token; replacing a reservation must remove its
    // old contribution. Replaying a terminal cancel cannot double-release it.
    for turn in 0..600 {
        let id = format!("order-{}", turn % 23);
        for pm in [&mut indexed, &mut scan] {
            let side = if turn % 2 == 0 { Side::Buy } else { Side::Sell };
            pm.register_pending_order_with_cash_fee(
                &id,
                if turn % 3 == 0 { "DN" } else { "UP" },
                side,
                0.37,
                9.0,
                if side == Side::Buy { 0.012 } else { 0.0 },
            );
            pm.apply_private_trade_reservation(&id, 4.0, 1);
            pm.apply_private_trade_reservation(&id, 2.0, -1);
            assert!(!pm.apply_private_trade_reservation(&id, 2.0, 0));
            if turn % 5 == 0 {
                pm.remove_pending_order(&id);
                pm.remove_pending_order(&id);
                assert!(!pm.apply_private_trade_reservation(&id, 4.0, -1));
            }
        }
        assert_equivalent(&indexed, &scan);
    }
    let mut replayed = PositionManager::from_snapshot(indexed.snapshot()).unwrap();
    replayed.enable_incremental_queries();
    assert_equivalent(&replayed, &scan);
    let before = indexed.available_cash();
    replayed.remove_pending_order("before");
    assert_eq!(
        indexed.available_cash(),
        before,
        "restored instances never share state"
    );
}

#[test]
fn reservation_index_tracks_lifecycle_partial_terminal_and_resurrection() {
    let mut scan = PositionManager::with_initial_quantities(HashMap::new(), 100.0);
    let mut indexed = PositionManager::with_initial_quantities(HashMap::new(), 100.0);
    indexed.enable_incremental_queries();
    for status in [
        OrderStatus::Accepted,
        OrderStatus::PartiallyFilled,
        OrderStatus::CancelUncertain,
        OrderStatus::Cancelled,
        OrderStatus::Cancelled,
        OrderStatus::Accepted,
        OrderStatus::Filled,
    ] {
        let mut update = super::tests::ou("order", Side::Buy, status, 0.4, 7.0);
        update.filled_quantity = 3.0;
        for pm in [&mut indexed, &mut scan] {
            pm.sync_pending_from_update(&update);
        }
        assert_equivalent(&indexed, &scan);
    }
}

#[test]
#[ignore = "paired private reservation benchmark; run release --nocapture --test-threads=1"]
fn private_reservation_history_latency_profile() {
    const N: usize = 20_000;
    for history in [0, 1_000, 10_000] {
        let mut scan =
            PositionManager::with_initial_quantities(HashMap::from([("UP".into(), 1e6)]), 1e6);
        for i in 0..history {
            let id = format!("historical-{i}");
            scan.register_pending_order(&id, "UP", Side::Buy, 0.4, 9.0);
            scan.apply_private_trade_reservation(&id, 9.0, 1);
        }
        scan.register_pending_order("current", "UP", Side::Buy, 0.4, 9.0);
        let mut indexed = PositionManager::from_snapshot(scan.snapshot()).unwrap();
        indexed.enable_incremental_queries();
        let mut samples = [Vec::with_capacity(N), Vec::with_capacity(N)];
        for i in 0..N + 256 {
            for version in [i % 2, 1 - i % 2] {
                let pm = if version == 0 {
                    &mut scan
                } else {
                    &mut indexed
                };
                let start = std::time::Instant::now();
                pm.apply_private_trade_reservation("current", 1.0, if i % 2 == 0 { 1 } else { -1 });
                std::hint::black_box((pm.available_cash(), pm.available_inventory("UP")));
                let ns = start.elapsed().as_nanos() as u64;
                if i >= 256 {
                    samples[version].push(ns);
                }
            }
        }
        assert_equivalent(&indexed, &scan);
        for (version, s) in samples.iter_mut().enumerate() {
            s.sort_unstable();
            eprintln!("private_reservation history={history} version={version} n={N} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=reservation_mutation_plus_available_cash_inventory excludes=trade_insert_transport_scheduling", s[N/2], s[N*99/100], s[N*999/1000], s[N-1]);
        }
    }
}
