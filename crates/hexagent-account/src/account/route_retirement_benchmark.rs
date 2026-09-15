//! Ignored release microbenchmark for successful GC route publication and its
//! moved destructor cost. No worker sleeps or production network traffic.
use super::*;
use std::hint::black_box;
use std::time::Instant;

fn report(boundary: &str, samples: &mut [u64], metrics: (usize, usize, u64, u64)) {
    samples.sort_unstable();
    let q = |p: usize| samples[(samples.len() * p).div_ceil(1000) - 1];
    eprintln!("route_retirement_benchmark boundary={boundary} n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} outstanding={} high_water={} backpressure={} reclaimed={} overflow=0",
        samples.len(), q(500), q(990), q(999), q(1000), metrics.0, metrics.1, metrics.2, metrics.3);
}

#[test]
#[ignore = "release benchmark: 50k retired routes, successful GC publication versus deferred drop"]
fn route_retirement_successful_gc_publication_benchmark() {
    const INITIAL: usize = 50_000;
    const WARMUP: usize = 64;
    const PER_EVENT: usize = SETTLED_GC_TRADES_PER_OWNER_TURN;
    let events = std::env::var("HEXAGENT_ROUTE_BENCH_SAMPLES")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(1000);
    assert!(events >= 1000 && events % BATCH_CAPACITY == 0);
    let initial: Vec<_> = (0..INITIAL)
        .map(|index| (format!("retired-trade-{index:064x}"), "btc01".to_string()))
        .collect();
    let changes: Vec<Vec<_>> = (0..events + WARMUP)
        .map(|event| {
            (0..PER_EVENT)
                .map(|offset| {
                    let index = INITIAL + event * PER_EVENT + offset;
                    (format!("retired-trade-{index:064x}"), "btc01".to_string())
                })
                .collect()
        })
        .collect();
    eprintln!("route_retirement_benchmark history_start={} history_end={} changes_per_event={} queued_batches_before_drain={} boundary=reserve_credit+group+clone+successful_RCU+publish+enqueue; cold_drop_and_retirement_age_separate; input_strings_and_seed_outside_timing; no_concurrent_RCU_writers",
        INITIAL + WARMUP * PER_EVENT, INITIAL + (WARMUP + events) * PER_EVENT,
        PER_EVENT, BATCH_CAPACITY);

    for deferred in [false, true] {
        let routes = ShardedRouteMap::new();
        routes.apply_batch("btc01", &[], &initial);
        let queue = RouteRetirementQueue::new();
        let mut publication = Vec::with_capacity(events);
        let mut cold_drop = Vec::with_capacity(events);
        let mut retirement_age = Vec::with_capacity(events);
        for (event, additions) in changes.iter().enumerate() {
            let started = Instant::now();
            if deferred {
                let mut permit = queue.try_reserve().expect("drain every eight batches");
                routes.apply_batch_retiring("btc01", &[], black_box(additions), Some(&mut permit));
                drop(permit);
            } else {
                routes.apply_batch("btc01", &[], black_box(additions));
            }
            let elapsed = started.elapsed().as_nanos() as u64;
            if event >= WARMUP {
                publication.push(elapsed);
            }

            // Account for the transferred cost explicitly, outside the private
            // publication boundary. Credit high-water includes eight queued
            // batches and any reader-held batch; this benchmark has no reader.
            if deferred && (event + 1) % BATCH_CAPACITY == 0 {
                for _ in 0..BATCH_CAPACITY {
                    let batch = queue.rx.try_recv().expect("eight completed publications");
                    let enqueued_at = batch.enqueued_at;
                    let mut pending = Some(batch);
                    let started = Instant::now();
                    queue.reclaim(&mut pending);
                    let elapsed = started.elapsed().as_nanos() as u64;
                    assert!(pending.is_none());
                    if event >= WARMUP {
                        cold_drop.push(elapsed);
                        retirement_age.push(enqueued_at.elapsed().as_nanos() as u64);
                    }
                }
            }
        }
        assert_eq!(
            routes
                .shards
                .iter()
                .map(|shard| shard.published.load().len())
                .sum::<usize>(),
            INITIAL + (events + WARMUP) * PER_EVENT
        );
        assert_eq!(queue.metrics().0, 0);
        report(
            if deferred {
                "gc_publication_deferred_drop"
            } else {
                "gc_publication_inline_drop"
            },
            &mut publication,
            queue.metrics(),
        );
        if deferred {
            report("cold_drop", &mut cold_drop, queue.metrics());
            report("retirement_age", &mut retirement_age, queue.metrics());
        }
    }
}

#[test]
#[ignore = "release benchmark: last load() reader release of a 3000-row route shard"]
fn route_retirement_last_reader_drop_benchmark() {
    const SHARD_ROWS: usize = 3000;
    const WARMUP: usize = 64;
    let events = std::env::var("HEXAGENT_ROUTE_BENCH_SAMPLES")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(1000);
    assert!(events >= 1000);
    // Private lookup holds one shard. Give that exact affected shard 3000
    // realistic-length keys; unrelated shards do not enter this boundary.
    let target_shard = ShardedRouteMap::shard_index("retired-trade-reader-target");
    let initial: Vec<_> = (0usize..)
        .map(|index| format!("retired-trade-{index:064x}"))
        .filter(|key| ShardedRouteMap::shard_index(key) == target_shard)
        .take(SHARD_ROWS)
        .map(|key| (key, "owner-a".to_string()))
        .collect();
    let key = initial[0].0.clone();
    let changes: Vec<Vec<_>> = (0..events + WARMUP)
        .map(|event| {
            vec![(
                key.clone(),
                if event % 2 == 0 { "owner-b" } else { "owner-a" }.to_string(),
            )]
        })
        .collect();
    let routes = [ShardedRouteMap::new(), ShardedRouteMap::new()];
    for map in &routes {
        map.apply_batch("owner-a", &[], &initial);
    }
    let queue = RouteRetirementQueue::new();
    let mut publication: [Vec<u64>; 2] = std::array::from_fn(|_| Vec::with_capacity(events));
    let mut reader_drop: [Vec<u64>; 2] = std::array::from_fn(|_| Vec::with_capacity(events));
    let mut cold_probe = Vec::with_capacity(events);
    let mut cold_drop = Vec::with_capacity(events);
    let mut retirement_age = Vec::with_capacity(events);
    eprintln!("route_retirement_benchmark held_reader=true affected_shard_rows={SHARD_ROWS} held_load_guards=1 changes_per_event=1 interleave_order=alternating reader_boundary=drop_last_ArcSwap_load_guard; cold_phase_on_same_benchmark_thread_outside_reader_boundary; no_concurrent_RCU_writers");

    for (event, additions) in changes.iter().enumerate() {
        // Alternate which variant runs first to reduce monotonic cache/CPU
        // drift. Each map receives exactly the same successful owner rebind.
        for variant in if event % 2 == 0 { [0, 1] } else { [1, 0] } {
            let deferred = variant == 1;
            let map = &routes[variant];
            let reader = map.shards[target_shard].published.load();
            assert_eq!(reader.len(), SHARD_ROWS);
            let started = Instant::now();
            if deferred {
                let mut permit = queue.try_reserve().unwrap();
                map.apply_batch_retiring("owner-a", &[], black_box(additions), Some(&mut permit));
                drop(permit);
            } else {
                map.apply_batch("owner-a", &[], black_box(additions));
            }
            let elapsed = started.elapsed().as_nanos() as u64;
            if event >= WARMUP {
                publication[variant].push(elapsed);
            }
            // RCU has paid this load() guard's debt. Without the queue the
            // guard is now the last strong ref, precisely the tail-risk case.
            assert_eq!(Arc::strong_count(&reader), if deferred { 2 } else { 1 });
            let mut pending = None;
            if deferred {
                let started = Instant::now();
                queue.reclaim(&mut pending);
                let elapsed = started.elapsed().as_nanos() as u64;
                assert!(pending.is_some(), "held reader prevents cold destruction");
                if event >= WARMUP {
                    cold_probe.push(elapsed);
                }
            }
            let started = Instant::now();
            drop(reader);
            let elapsed = started.elapsed().as_nanos() as u64;
            if event >= WARMUP {
                reader_drop[variant].push(elapsed);
            }

            if deferred {
                let enqueued_at = pending.as_ref().unwrap().enqueued_at;
                let started = Instant::now();
                queue.reclaim(&mut pending);
                let elapsed = started.elapsed().as_nanos() as u64;
                assert!(pending.is_none());
                if event >= WARMUP {
                    cold_drop.push(elapsed);
                    retirement_age.push(enqueued_at.elapsed().as_nanos() as u64);
                }
            }
        }
    }
    report(
        "held_reader_publication_inline",
        &mut publication[0],
        (0, 0, 0, 0),
    );
    report(
        "held_reader_publication_deferred",
        &mut publication[1],
        queue.metrics(),
    );
    report("last_reader_drop_inline", &mut reader_drop[0], (0, 0, 0, 0));
    report(
        "last_reader_drop_deferred",
        &mut reader_drop[1],
        queue.metrics(),
    );
    report("held_reader_cold_probe", &mut cold_probe, queue.metrics());
    report("held_reader_cold_drop", &mut cold_drop, queue.metrics());
    report(
        "held_reader_retirement_age",
        &mut retirement_age,
        queue.metrics(),
    );
    assert_eq!(queue.metrics().0, 0);
}
