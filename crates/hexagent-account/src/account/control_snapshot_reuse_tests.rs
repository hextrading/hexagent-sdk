use super::*;

fn fixture(tokens: usize) -> SharedAccountState {
    let mut state = SharedAccountState::default();
    state.physical_cash = 500.0;
    state.instances.insert("a".into(), InstanceLedger::new(1.0));
    state.instances.get_mut("a").unwrap().cash = 300.0;
    state.instances.insert("b".into(), InstanceLedger::new(1.0));
    state.instances.get_mut("b").unwrap().cash = 100.0;
    for index in 0..tokens {
        let token = format!("{index:064}");
        state.settled_token_values.insert(token.clone(), (index % 2) as f64);
        state.physical_positions.insert(token.clone(), 10.0);
        state.instances.get_mut("a").unwrap().positions.insert(token.clone(), 6.0);
        state.instances.get_mut("b").unwrap().positions.insert(token, 4.0);
    }
    state
}

#[test]
fn borrowed_reconciliation_matches_original_with_pending_and_missing_tokens() {
    let mut state = fixture(500);
    state.instances.get_mut("a").unwrap().positions.insert("virtual-only".into(), 9.0);
    state.physical_positions.insert("physical-only".into(), 2.0);
    state.provisional_position_owners.insert("missing".into(), "a".into());
    let mut pending = PendingPhysicalDeltas { cash: 4.5, ..Default::default() };
    pending.positions.insert("virtual-only".into(), 3.0);
    pending.positions.insert(format!("{:064}", 0), -2.0);
    for _ in 0..3 {
        let mut original = state.clone();
        legacy_recompute(&mut original, "reference", &pending);
        recompute_reconciliation_with_pending(&mut state, "optimized", &pending);
        assert_eq!(state.unallocated_cash, original.unallocated_cash);
        assert_eq!(state.unallocated_positions, original.unallocated_positions);
        assert_eq!(state.provisional_position_owners, original.provisional_position_owners);
        assert_eq!(state.uncertain, original.uncertain);
        assert_eq!(state.uncertain_reason, original.uncertain_reason);
    }
}

#[test]
#[ignore = "focused cold owner collection benchmark; release single test thread"]
fn benchmark_cold_owner_token_collections() {
    const N: usize = 2000;
    for operation in ["reconciliation"] {
        for legacy in [true, false] {
            let mut state = fixture(4000);
            let pending = PendingPhysicalDeltas::default();
            let mut run = || {
                if legacy { legacy_recompute(&mut state, "bench", &pending); }
                else { recompute_reconciliation_with_pending(&mut state, "bench", &pending); }
                std::hint::black_box(&state);
            };
            for _ in 0..32 { run(); }
            let mut samples = Vec::with_capacity(N);
            for _ in 0..N { let started = std::time::Instant::now(); run(); samples.push(started.elapsed().as_nanos()); }
            samples.sort_unstable();
            eprintln!("cold_owner operation={operation} legacy={legacy} tokens=4000 instances=2 n={N} unit=ns p50={} p99={} p999={} max={} queue_depth=0 overflow=0 boundary=exact_collection_operation_on_single_cold_owner",
                samples[N/2-1], samples[N*99/100-1], samples[N*999/1000-1], samples[N-1]);
        }
    }
}

fn legacy_recompute(
    state: &mut SharedAccountState,
    _deficit_context: &str,
    pending: &PendingPhysicalDeltas,
) {
    state.provisional_position_owners.retain(|token, owner| {
        state
            .instances
            .get(owner)
            .and_then(|instance| instance.positions.get(token))
            .is_some_and(|quantity| *quantity > EPS)
    });
    let virtual_cash: f64 = state.instances.values().map(|instance| instance.cash).sum();
    // MATCHED is the earliest reliable inventory edge for quoting, but the
    // Polygon wallet does not change until MINED/CONFIRMED. Exclude those
    // explicitly pending physical deltas from reconciliation so a perfectly
    // healthy in-flight settlement does not look like missing cash/shares.
    state.unallocated_cash = state.physical_cash - (virtual_cash - pending.cash);
    state.unallocated_positions.clear();
    let mut all_tokens: HashSet<String> = state.physical_positions.keys().cloned().collect();
    all_tokens.extend(
        state
            .instances
            .values()
            .flat_map(|instance| instance.positions.keys().cloned()),
    );
    for token in all_tokens {
        let physical = state.physical_positions.get(&token).copied().unwrap_or(0.0);
        let virtual_qty: f64 = state
            .instances
            .values()
            .map(|instance| instance.positions.get(&token).copied().unwrap_or(0.0))
            .sum();
        let pending_quantity = pending.positions.get(&token).copied().unwrap_or(0.0);
        let expected_virtual = virtual_qty - pending_quantity;
        let delta = physical - expected_virtual;
        let tolerance = reconciliation_tolerance(physical, expected_virtual);
        if delta.abs() > tolerance {
            state.unallocated_positions.insert(token, delta);
        }
    }
    recompute_reconciliation_status(state);
}

