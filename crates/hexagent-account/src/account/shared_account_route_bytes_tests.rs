use super::*;
use std::hint::black_box;

#[test]
fn route_snapshots_share_unchanged_bytes_and_detach_owner_rebinds() {
    let routes = ShardedRouteMap::new();
    let key = "original-key";
    let sibling = (0..10_000)
        .map(|i| format!("same-shard-{i}"))
        .find(|k| ShardedRouteMap::shard_index(k) == ShardedRouteMap::shard_index(key))
        .unwrap();
    routes.insert(key.into(), "owner".into());
    let shard = &routes.shards[ShardedRouteMap::shard_index(key)];
    let first = shard.published.load_full();
    routes.insert(sibling, "sibling-owner".into());
    let second = shard.published.load_full();
    let (first_key, first_owner) = first.get_key_value(key).unwrap();
    let (second_key, second_owner) = second.get_key_value(key).unwrap();
    assert!(Arc::ptr_eq(first_key, second_key));
    assert!(Arc::ptr_eq(first_owner, second_owner));
    routes.apply_batch("owner", &[], &[(key.into(), "new-owner".into())]);
    assert_eq!(first.get(key).unwrap().as_ref(), "owner");
    assert_eq!(routes.get(key).as_deref(), Some("new-owner"));
    routes.apply_batch("owner", &[key.into()], &[]);
    assert_eq!(
        routes.get(key).as_deref(),
        Some("new-owner"),
        "stale owner removal cannot remove a rebound route"
    );
    routes.apply_batch("new-owner", &[key.into()], &[]);
    assert_eq!(routes.get(key), None);
    assert_eq!(second.get(key).unwrap().as_ref(), "owner");
}

#[test]
fn concurrent_route_batches_preserve_other_owners_and_held_snapshots() {
    let routes = Arc::new(ShardedRouteMap::new());
    routes.insert("held".into(), "original".into());
    let held = routes.shards[ShardedRouteMap::shard_index("held")]
        .published
        .load_full();
    let workers = (0..4)
        .map(|owner| {
            let routes = Arc::clone(&routes);
            std::thread::spawn(move || {
                let owner = format!("owner-{owner}");
                for batch in 0..100 {
                    let rows = (0..8)
                        .map(|i| (format!("{owner}-{batch}-{i}"), owner.clone()))
                        .collect::<Vec<_>>();
                    routes.apply_batch(&owner, &[], &rows);
                    assert!(
                        rows.iter()
                            .all(|(key, _)| routes.get(key).as_deref() == Some(owner.as_str()))
                    );
                }
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(routes.keys().len(), 3201);
    assert_eq!(held.get("held").unwrap().as_ref(), "original");
    for owner in 0..4 {
        for batch in 0..100 {
            for i in 0..8 {
                assert_eq!(
                    routes.get(&format!("owner-{owner}-{batch}-{i}")),
                    Some(format!("owner-{owner}"))
                );
            }
        }
    }
}

fn summary(label: &str, values: &mut [u64]) {
    values.sort_unstable();
    let q = |p: usize| values[(values.len() * p).div_ceil(1000) - 1];
    eprintln!(
        "route_bytes_benchmark boundary={label} n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0",
        values.len(),
        q(500),
        q(990),
        q(999),
        q(1000)
    );
}

// String-map batch algorithm from SDK e3cd44c, before shared bytes.
// The no-op guard is omitted because this fixture always changes every batch.
fn legacy_batch(
    shards: &[ArcSwap<HashMap<String, String>>],
    removals: &[String],
    additions: &[(String, String)],
) {
    let mut grouped: [Vec<(&str, Option<&str>)>; ROUTE_SHARD_COUNT] =
        std::array::from_fn(|_| Vec::new());
    for key in removals {
        grouped[ShardedRouteMap::shard_index(key)].push((key, None));
    }
    for (key, owner) in additions {
        grouped[ShardedRouteMap::shard_index(key)].push((key, Some(owner)));
    }
    for (index, changes) in grouped.iter().enumerate().filter(|(_, x)| !x.is_empty()) {
        shards[index].rcu(|current| {
            let mut next = (**current).clone();
            for (key, value) in changes {
                if let Some(value) = value {
                    next.insert((*key).to_owned(), (*value).to_owned());
                } else if next.get(*key).is_some_and(|v| v == "owner") {
                    next.remove(*key);
                }
            }
            Arc::new(next)
        });
    }
}

#[test]
#[ignore = "45,000-route GC batch and lookup before/after release benchmark"]
fn benchmark_route_shard_shared_bytes() {
    const N: usize = 1000;
    let rows = (0..45_000)
        .map(|i| (format!("{i:064x}"), "owner".to_string()))
        .collect::<Vec<_>>();
    let mut maps: [HashMap<String, String>; ROUTE_SHARD_COUNT] =
        std::array::from_fn(|_| HashMap::new());
    for (key, owner) in &rows {
        maps[ShardedRouteMap::shard_index(key)].insert(key.clone(), owner.clone());
    }
    let modern = ShardedRouteMap::new();
    for (i, map) in maps.iter().enumerate() {
        modern.shards[i].published.store(Arc::new(
            map.iter()
                .map(|(k, v)| (Arc::from(k.as_str()), Arc::from(v.as_str())))
                .collect(),
        ));
    }
    let legacy = maps
        .into_iter()
        .map(ArcSwap::from_pointee)
        .collect::<Vec<_>>();
    let mut before = Vec::with_capacity(N);
    let mut after = Vec::with_capacity(N);
    let mut read_before = Vec::new();
    let mut read_after = Vec::new();
    let mut removals = rows[..8].iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    for i in 0..N {
        let additions = (0..8)
            .map(|j| (format!("{:064x}", 45_000 + i * 8 + j), "owner".into()))
            .collect::<Vec<_>>();
        let t = Instant::now();
        legacy_batch(&legacy, &removals, &additions);
        before.push(t.elapsed().as_nanos() as u64);
        let t = Instant::now();
        modern.apply_batch("owner", &removals, &additions);
        after.push(t.elapsed().as_nanos() as u64);
        removals = additions.into_iter().map(|(k, _)| k).collect();
        let key = &rows[10 + i].0;
        let t = Instant::now();
        black_box(
            legacy[ShardedRouteMap::shard_index(key)]
                .load()
                .get(key)
                .cloned(),
        );
        read_before.push(t.elapsed().as_nanos() as u64);
        let t = Instant::now();
        black_box(modern.get(key));
        read_after.push(t.elapsed().as_nanos() as u64);
    }
    summary("batch_8_remove_8_insert_string_45000", &mut before);
    summary("batch_8_remove_8_insert_shared_45000", &mut after);
    summary("lookup_string", &mut read_before);
    summary("lookup_shared", &mut read_after);
    assert_eq!(modern.keys().len(), 45_000);
}
