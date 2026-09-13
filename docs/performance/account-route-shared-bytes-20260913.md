# GC route snapshot allocation — 2026-09-13

The first indexed/batched release (SDK e3cd44c, hexbot f8b4eee) still exhibited a 26.10 ms GC owner attempt in maker02's 15:40 UTC histogram window: the successful critical section was 26.08 ms, including 26.02 ms in route publication, versus 22.5 µs selection and 191.5 µs typed persistence capture. Batching reduced repeated publication but each changed shard still deep-cloned every historical key and owner String.

A subsequent 499 Hz production perf capture (15:43:40–15:46:41 UTC, four selected account/writer threads) contained 1,740 CPU samples, zero lost records and five GC-path samples. The private lifecycle actor stack includes `process_settled_gc_delete_request -> ShardedRouteMap::apply_batch -> HashMap::clone`, with memcpy, mimalloc allocation/purge, madvise and page-fault handling. There were 102 GC-path madvise calls in a roughly 1.25 ms cluster. This identifies avoidable allocation in the correct path; that later cluster alone does not explain the entire earlier 26 ms event.

Shard snapshots now store immutable `Arc<str>` keys and owners. Publishing a changed shard copies table entries and reference counts while sharing unchanged string bytes. Rebinding creates a new immutable owner; held readers retain their old snapshot. Conditional owner deletion and ArcSwap's retry semantics remain intact. Public lookups still return an owned String, preserving the caller contract.

## Measurement

Full SDK release benchmark: `cargo test -p hexagent-account --release --lib benchmark_route_shard_shared_bytes -- --ignored --nocapture`, macOS x86_64, fat LTO, 1,000 alternating samples per variant. The fixture holds 45,000 64-byte keys across 64 shards; each batch removes eight keys and inserts eight replacements. Grouping, RCU publication and synchronous retirement are inside the timer; fixture and input creation are outside. The old variant reproduces the e3cd44c String-map batch without its no-op guard (every fixture batch changes). Lookup includes shard hashing, ArcSwap load and creation/destruction of the returned String in both variants. Units: microseconds.

| Boundary | N | P50 | P99 | P999 | Maximum |
|---|---:|---:|---:|---:|---:|
| String batch | 1,000 | 3935.103 | 8255.448 | 10408.599 | 11458.540 |
| Shared-byte batch | 1,000 | 889.010 | 1636.946 | 2072.456 | 3138.460 |
| String lookup | 1,000 | 2.401 | 5.111 | 43.022 | 70.684 |
| Shared-byte lookup | 1,000 | 2.657 | 7.067 | 25.131 | 32.977 |

Batch P99 improves about 80%; this is component evidence, not production end-to-end improvement. Lookup median/P99 increased slightly in this run; no read-path speedup is claimed. Queue depth/overflow are 0/0 by construction in this synchronous benchmark. Production measurement must separately cover lifecycle waiting, private-to-strategy application, quote/dispatch and overflow so reduced GC cost cannot hide downstream regression.

## Ownership, correctness and remaining migration

The existing private lifecycle actor owns ledger mutation. The cold settled-ledger mailbox initiates deletion through the existing command lane; it does not execute the bound deletion itself. StrategyAccount remains owned solely by its strategy thread. Route readers receive immutable snapshots; there are no new workers, queues, mutexes, synchronous requests, affinity changes or quote-path operations. GC/persistence priorities, bounds, generation ordering, replay and fail-closed overflow behavior are unchanged from the preceding release.

Cloning a shard still allocates one hash table and touches reference counts. New route creation still allocates its key/owner. This change removes per-historical-string allocation, not all allocation from the legacy lifecycle subsystem. If production evidence still shows material retirement cost, the next migration should prepare immutable shards off the lifecycle lane with owner-epoch validation, or move route deltas to a dedicated publisher while preserving private-event lookup readiness and lossless ordering. Do not move live account mutation to the cold worker or weaken durability to reduce timings.

Validation includes held-snapshot byte sharing, owner rebind and stale-owner deletion, four concurrent owners publishing 400 batches without lost routes, and the full account suite (251 passed, eight opt-in benchmarks ignored). Existing ordering, replay/idempotence, queue overflow and instance-isolation tests remain enabled. Exchange validation ran all 627 tests successfully as 626 in one serial process plus the retired-taker replay test in isolation (11 opt-in tests ignored). The complete debug process and the 46-test user-feed group reproduce a two-second retired-taker barrier timeout; the unchanged preceding SDK source reproduces the same 45-pass/one-timeout user-feed result. The preceding release binary passes all 627 tests. This pre-existing debug suite interaction remains a validation limitation; no timeout or correctness assertion was relaxed.
