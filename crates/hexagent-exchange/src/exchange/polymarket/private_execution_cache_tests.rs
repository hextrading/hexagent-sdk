use super::*;
use crate::types::{FeeBasis, FeeSettlement, OrderSlot, TradeFee};
use hexagent_account::account::shared_account::TradeOwnership;

fn execution(rate: f64) -> FrozenTradeExecution {
    FrozenTradeExecution::new(
        0.16,
        15.0,
        2.4327,
        Side::Sell,
        false,
        FeeBasis {
            settlement: FeeSettlement::CollateralV2,
            rate,
            exponent: 1.0,
        },
    )
    .unwrap()
}

fn seed(
    trade: &str,
    status: &str,
    execution: Option<FrozenTradeExecution>,
) -> PrivateExecutionSeed {
    PrivateExecutionSeed {
        ownership: TradeOwnership {
            order_slot: OrderSlot::default(),
            account_id: "account".into(),
            instance_id: "owner".into(),
            trade_key: trade.into(),
            client_order_id: "client-order".into(),
            order_id: "0xabcdef".into(),
            token_id: "DOWN".into(),
            side: Side::Sell,
            quantity: 15.0,
            price: execution.map_or(0.16, |value| value.price),
            status: status.into(),
        },
        is_maker: false,
        execution,
    }
}

fn insert(
    cache: &mut PrivateExecutionCache,
    trade: &str,
    value: FrozenTradeExecution,
) -> Result<(), String> {
    cache.insert(trade, "0xabcdef", "DOWN", Side::Sell, 15.0, false, value)
}

fn lookup(cache: &PrivateExecutionCache, trade: &str) -> Option<FrozenTradeExecution> {
    cache
        .lookup(trade, "0xabcdef", "DOWN", Side::Sell, 15.0, 0.16, false)
        .unwrap()
}

fn full_unacknowledged_cache() -> PrivateExecutionCache {
    let mut cache = PrivateExecutionCache::new(Vec::new()).unwrap();
    let value = execution(0.07);
    for index in 0..CAPACITY {
        insert(&mut cache, &format!("trade-{index}"), value).unwrap();
    }
    cache
}

#[test]
fn capacity_preserves_every_nonterminal_and_unacknowledged_execution() {
    let mut cache = full_unacknowledged_cache();
    let before = cache.rows.len();
    assert!(insert(&mut cache, "overflow", execution(0.08)).is_err());
    assert_eq!(cache.rows.len(), before);
    assert!(cache.reclaimable.is_empty());
    assert_eq!(lookup(&cache, "trade-0"), Some(execution(0.07)));
    assert!(lookup(&cache, "overflow").is_none());

    // A nonterminal cold ACK cannot release the economics certificate.
    cache.acknowledge(PrivateRouteIdentity::TradeLifecycle {
        fingerprint: key("trade-0", "0xabcdef", false),
        rank: 2,
    });
    assert!(insert(&mut cache, "still-overflow", execution(0.08)).is_err());
    assert!(cache.reclaimable.is_empty());

    // A terminal observation alone must not count as durable acknowledgement.
    // The production acknowledge() method sets both bits atomically on the owner.
    cache
        .rows
        .get_mut(&key("trade-0", "0xabcdef", false))
        .unwrap()
        .terminal = true;
    assert!(insert(&mut cache, "terminal-without-ack", execution(0.08)).is_err());
    assert_eq!(lookup(&cache, "trade-0"), Some(execution(0.07)));
}

#[test]
fn terminal_cold_ack_reclaims_once_without_growing_preallocated_storage() {
    let mut cache = full_unacknowledged_cache();
    let row_capacity = cache.rows.capacity();
    let queue_capacity = cache.reclaimable.capacity();
    let acknowledged = PrivateRouteIdentity::TradeLifecycle {
        fingerprint: key("trade-0", "0xabcdef", false),
        rank: 3,
    };
    cache.acknowledge(acknowledged);
    cache.acknowledge(acknowledged);
    cache.acknowledge(PrivateRouteIdentity::TradeLifecycle {
        fingerprint: key("never-inserted", "0xabcdef", false),
        rank: 4,
    });
    assert_eq!(cache.reclaimable.len(), 1);
    insert(&mut cache, "new-trade", execution(0.08)).unwrap();
    assert_eq!(cache.rows.len(), CAPACITY);
    assert!(lookup(&cache, "trade-0").is_none());
    assert_eq!(lookup(&cache, "new-trade"), Some(execution(0.08)));
    assert_eq!(lookup(&cache, "trade-1"), Some(execution(0.07)));
    assert_eq!(cache.rows.capacity(), row_capacity);
    assert_eq!(cache.reclaimable.capacity(), queue_capacity);
    assert!(insert(&mut cache, "no-second-victim", execution(0.08)).is_err());
    // This cache-only test does not assert historical FAILED replay delivery;
    // a reclaimed durable identity needs separate coordinator coverage.
}

#[test]
fn startup_legacy_economics_remain_frozen_when_new_execution_is_more_precise() {
    let legacy = FrozenTradeExecution {
        raw_price: 0.16,
        price: 0.16,
        gross_notional: None,
        fee_basis: FeeBasis {
            settlement: FeeSettlement::CollateralV2,
            rate: 0.07,
            exponent: 1.0,
        },
        fee: TradeFee {
            settlement: FeeSettlement::CollateralV2,
            usdc_fee: 0.14112,
            shares_fee: 0.0,
        },
    };
    let mut cache =
        PrivateExecutionCache::new(vec![seed("historical", "CONFIRMED", Some(legacy))]).unwrap();
    insert(&mut cache, "historical", execution(0.08)).unwrap();
    assert_eq!(lookup(&cache, "historical"), Some(legacy));
    assert_eq!(cache.reclaimable.len(), 1);
    assert!(cache
        .lookup(
            "historical",
            "0xabcdef",
            "DOWN",
            Side::Sell,
            15.0,
            0.16218,
            false
        )
        .is_err());
}

#[test]
fn startup_pending_fee_never_becomes_new_zero_fee_execution() {
    let cache = PrivateExecutionCache::new(vec![seed("pending", "MATCHED", None)]).unwrap();
    assert!(cache
        .lookup("pending", "0xabcdef", "DOWN", Side::Sell, 15.0, 0.16, false)
        .is_err());
    assert!(cache.reclaimable.is_empty());
}

#[test]
fn startup_mined_trade_keeps_original_economics_until_failed_cold_ack() {
    let original = FrozenTradeExecution {
        raw_price: 0.16,
        price: 0.16,
        gross_notional: None,
        fee_basis: FeeBasis {
            settlement: FeeSettlement::CollateralV2,
            rate: 0.07,
            exponent: 1.0,
        },
        fee: TradeFee {
            settlement: FeeSettlement::CollateralV2,
            usdc_fee: 0.14112,
            shares_fee: 0.0,
        },
    };
    let mut cache =
        PrivateExecutionCache::new(vec![seed("mined-at-restart", "MINED", Some(original))])
            .unwrap();
    assert!(cache.reclaimable.is_empty());
    cache.acknowledge(PrivateRouteIdentity::TradeLifecycle {
        fingerprint: key("mined-at-restart", "0xabcdef", false),
        rank: 2,
    });
    assert!(cache.reclaimable.is_empty());
    // The first post-restart event can be FAILED. Its route must still carry
    // the same legacy principal/fee, even if current normalization improves it.
    insert(&mut cache, "mined-at-restart", execution(0.08)).unwrap();
    assert_eq!(lookup(&cache, "mined-at-restart"), Some(original));
    cache.acknowledge(PrivateRouteIdentity::TradeLifecycle {
        fingerprint: key("mined-at-restart", "0xabcdef", false),
        rank: 4,
    });
    assert_eq!(cache.reclaimable.len(), 1);
    assert_eq!(lookup(&cache, "mined-at-restart"), Some(original));
}

#[test]
fn separate_owner_caches_and_identity_checks_prevent_cross_instance_economics() {
    let mut owner = PrivateExecutionCache::new(Vec::new()).unwrap();
    let mut sibling = PrivateExecutionCache::new(Vec::new()).unwrap();
    insert(&mut owner, "same-id", execution(0.07)).unwrap();
    assert!(lookup(&sibling, "same-id").is_none());
    insert(&mut sibling, "same-id", execution(0.08)).unwrap();
    assert_eq!(lookup(&owner, "same-id"), Some(execution(0.07)));
    assert_eq!(lookup(&sibling, "same-id"), Some(execution(0.08)));
    for (order, token, side, quantity) in [
        ("different-order", "DOWN", Side::Sell, 15.0),
        ("0xabcdef", "UP", Side::Sell, 15.0),
        ("0xabcdef", "DOWN", Side::Buy, 15.0),
        ("0xabcdef", "DOWN", Side::Sell, 14.0),
    ] {
        assert!(owner
            .lookup("same-id", order, token, side, quantity, 0.16, false)
            .is_err());
    }
    assert_eq!(
        owner
            .lookup(
                "same-id",
                "  0XABCDEF  ",
                "DOWN",
                Side::Sell,
                15.0,
                0.16,
                false
            )
            .unwrap(),
        Some(execution(0.07))
    );
}

#[test]
fn metadata_change_applies_only_to_new_identity_and_duplicate_never_rebinds() {
    let mut cache = PrivateExecutionCache::new(Vec::new()).unwrap();
    insert(&mut cache, "old", execution(0.07)).unwrap();
    let newer_metadata = execution(0.08);
    insert(&mut cache, "old", newer_metadata).unwrap();
    insert(&mut cache, "next", newer_metadata).unwrap();
    assert_eq!(lookup(&cache, "old"), Some(execution(0.07)));
    assert_eq!(lookup(&cache, "next"), Some(newer_metadata));
    assert_ne!(
        lookup(&cache, "old").unwrap().fee,
        lookup(&cache, "next").unwrap().fee
    );
}

#[test]
fn oversized_startup_seed_fails_before_runtime() {
    let rows = vec![seed("old", "CONFIRMED", Some(execution(0.07))); CAPACITY + 1];
    assert!(PrivateExecutionCache::new(rows).is_err());
}

#[test]
fn lost_advisory_ack_still_reclaims_from_exact_completion_slot() {
    let mut cache = full_unacknowledged_cache();
    let identity = PrivateRouteIdentity::TradeLifecycle {
        fingerprint: key("trade-0", "0xabcdef", false),
        rank: 3,
    };
    let ticket = cache.ticket(identity).unwrap();
    cache.ack_lane().acknowledge(ticket);
    // Deliberately do not deliver cache.acknowledge(identity): its advisory
    // channel may be full. The retained completion certificate is sufficient.
    assert!(cache.reclaimable.is_empty());
    insert(&mut cache, "after-lost-hint", execution(0.08)).unwrap();
    assert!(lookup(&cache, "trade-0").is_none());
    assert_eq!(lookup(&cache, "after-lost-hint"), Some(execution(0.08)));
    assert_eq!(cache.rows.len(), CAPACITY);
}

#[test]
fn delayed_old_completion_cannot_reclaim_a_reused_slot() {
    let mut cache = full_unacknowledged_cache();
    let old = cache
        .ticket(PrivateRouteIdentity::TradeLifecycle {
            fingerprint: key("trade-0", "0xabcdef", false),
            rank: 3,
        })
        .unwrap();
    let ack = cache.ack_lane();
    ack.acknowledge(old);
    insert(&mut cache, "new-slot-owner", execution(0.08)).unwrap();
    let current = cache
        .ticket(PrivateRouteIdentity::TradeLifecycle {
            fingerprint: key("new-slot-owner", "0xabcdef", false),
            rank: 3,
        })
        .unwrap();
    assert_eq!(current.slot, old.slot);
    assert!(current.generation > old.generation);
    ack.acknowledge(old);
    assert!(insert(&mut cache, "must-remain-full", execution(0.08)).is_err());
    assert_eq!(lookup(&cache, "new-slot-owner"), Some(execution(0.08)));
    ack.acknowledge(current);
    // An older ACK arriving after the current one cannot regress its proof.
    ack.acknowledge(old);
    insert(&mut cache, "after-current-ack", execution(0.08)).unwrap();
    assert!(lookup(&cache, "new-slot-owner").is_none());
}

#[test]
fn cold_fee_repair_is_identity_checked_frozen_and_idempotent() {
    let mut cache = PrivateExecutionCache::new(vec![seed("repair", "MINED", None)]).unwrap();
    assert!(cache.needs_repair("repair", "0xabcdef", false));
    let pending = seed("repair", "MINED", None);
    assert!(cache.repair(pending).is_err());
    let authoritative = seed("repair", "MINED", Some(execution(0.07)));
    let mut wrong_owner = authoritative.clone();
    wrong_owner.ownership.token_id = "UP".into();
    assert!(cache.repair(wrong_owner).is_err());
    assert!(cache.needs_repair("repair", "0xabcdef", false));
    cache.repair(authoritative.clone()).unwrap();
    cache.repair(authoritative).unwrap();
    assert!(!cache.needs_repair("repair", "0xabcdef", false));
    assert_eq!(lookup(&cache, "repair"), Some(execution(0.07)));
    assert!(cache
        .repair(seed("repair", "MINED", Some(execution(0.08))))
        .is_err());
    assert_eq!(lookup(&cache, "repair"), Some(execution(0.07)));
    assert!(cache.reclaimable.is_empty());
}

#[test]
fn terminal_pending_fee_cannot_be_evicted_before_repaired_owner_delivery() {
    let mut cache =
        PrivateExecutionCache::new(vec![seed("terminal-pending", "CONFIRMED", None)]).unwrap();
    for index in 1..CAPACITY {
        insert(&mut cache, &format!("unconfirmed-{index}"), execution(0.07)).unwrap();
    }
    assert!(insert(&mut cache, "before-repair", execution(0.08)).is_err());
    assert!(cache.needs_repair("terminal-pending", "0xabcdef", false));
    cache
        .repair(seed("terminal-pending", "CONFIRMED", Some(execution(0.07))))
        .unwrap();
    assert!(cache.needs_delivery("terminal-pending", "0xabcdef", false));
    assert!(insert(&mut cache, "before-owner-delivery", execution(0.08)).is_err());
    let identity = PrivateRouteIdentity::TradeLifecycle {
        fingerprint: key("terminal-pending", "0xabcdef", false),
        rank: 3,
    };
    cache.mark_delivered(identity);
    cache.acknowledge(identity);
    insert(&mut cache, "after-owner-delivery", execution(0.08)).unwrap();
    assert!(lookup(&cache, "terminal-pending").is_none());
    assert_eq!(
        lookup(&cache, "after-owner-delivery"),
        Some(execution(0.08))
    );
}

#[test]
#[ignore = "bounded cache saturation CPU benchmark; run alone with --nocapture --test-threads=1"]
fn private_execution_cache_saturation_benchmark() {
    use std::hint::black_box;
    use std::time::Instant;
    const N: usize = 2_000;
    let summary = |mut values: Vec<u64>| {
        values.sort_unstable();
        let at = |quantile: f64| values[(quantile * values.len() as f64).ceil() as usize - 1];
        serde_json::json!({
            "n":values.len(), "p50_ns":at(0.5), "p99_ns":at(0.99),
            "p999_ns":at(0.999), "max_ns":values[values.len()-1],
        })
    };
    let mut blocked = full_unacknowledged_cache();
    let value = execution(0.07);
    let mut failures = Vec::with_capacity(N);
    for _ in 0..100 {
        assert!(insert(&mut blocked, "overflow", value).is_err());
    }
    for _ in 0..N {
        let start = Instant::now();
        let result = insert(black_box(&mut blocked), black_box("overflow"), value);
        let elapsed = start.elapsed().as_nanos() as u64;
        assert!(result.is_err());
        failures.push(elapsed);
    }
    let mut recoverable = full_unacknowledged_cache();
    let ack = recoverable.ack_lane();
    let mut victim = "trade-0".to_string();
    let mut recovered = Vec::with_capacity(N);
    for index in 0..N + 100 {
        let ticket = recoverable
            .ticket(PrivateRouteIdentity::TradeLifecycle {
                fingerprint: key(&victim, "0xabcdef", false),
                rank: 3,
            })
            .unwrap();
        ack.acknowledge(ticket);
        let incoming = format!("replacement-{index}");
        let start = Instant::now();
        let result = insert(black_box(&mut recoverable), black_box(&incoming), value);
        let elapsed = start.elapsed().as_nanos() as u64;
        result.unwrap();
        if index >= 100 {
            recovered.push(elapsed);
        }
        victim = incoming;
    }
    println!(
        "PRIVATE_CACHE_BENCH {}",
        serde_json::json!({
            "capacity":CAPACITY,"warmup":100,
            "preallocated_hashmap_capacity":recoverable.rows.capacity(),
            "row_value_size_bytes":std::mem::size_of::<CachedExecution>(),
            "row_key_value_size_bytes":std::mem::size_of::<(u128,CachedExecution)>(),
            "full_without_ack":summary(failures),"lost_hint_exact_ack_recovery":summary(recovered),
            "boundary":"insert into full private owner cache; includes bounded scan/reclaim; no concurrent producers",
            "queue_depth":null,"queue_overflow":null,
            "excludes":["table construction","ticket ACK publication","incoming string allocation","cold ledger","quote","network"],
            "note":"New bounded-table pressure path has no old implementation equivalent; not a before/after queue benchmark. P999 has only two tail samples at N=2000.",
        })
    );
}
