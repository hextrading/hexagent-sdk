# Settled GC identity publication

The first account-queue release removed cold-lock parking and all-slot runtime
ownership RCU updates. Production still showed a 46.78 ms GC turn, followed by
a 19.45 ms turn in the next event rollover. A four-minute, two-thread perf
capture of that second rollover found full identity-map rebuilding both in
`ExecutionStateOwner::apply` and in `SharedState::run_settled_gc_pass`.

`RetireMappings` previously cloned and rebuilt all three identity maps. Final
GC also rebuilt all three, including the case where event eviction had already
removed every relevant mapping. The new implementation returns the identities
actually removed from the owner's mutable maps, clones only affected immutable
shards once per batch, and publishes the three consistent maps together. A
repeated retirement with no actual deletion does not publish a new snapshot.

## Ownership and correctness

- The existing lifecycle owner remains the sole writer of execution state.
  Removed-key vectors are local to that owner turn; there is no new worker,
  queue, shared mutable authority, scheduling policy or topology assignment.
- Per-instance/token filtering, normalized exchange-order IDs, private event
  ordering and the runtime ownership index's existing cleanup remain intact.
  GC certificates, epoch checks, wallet grace and persistence are unchanged.
- Readers continue consuming one immutable snapshot. Every affected shard is
  copied before modification; unrelated shards and old reader generations are
  retained. Duplicate/missing deletion keys cannot decrement length twice.
- The return structure moves the existing selected client keys and retains the
  normalized OIDs already needed for owner-map removal. This replaces full-map
  allocation/rehashing on the existing retirement lane; it adds no work to quote
  calculation or execution dispatch.
- Capacity, priority, overflow and recovery semantics of all existing message
  queues are unchanged. Repeated commands remain idempotent and later replay
  routes outside the retired token scope remain routable.

## Measurement boundaries

Production capture: 12:47:18–12:51:18 UTC, SDK `d336bce2`, hexbot `f2bc1fa9`,
PID 45971. CPU 499 Hz on the cold account and private lifecycle owners only;
the collector used CPU 0–1, normal scheduling, nice 10. There were 1,116 CPU
samples (969 cold, 147 lifecycle), with full rebuild stacks around 12:50:02 and
12:50:04. This captured the 19.45 ms event, not the earlier 46.78 ms event.
The higher-frequency capture overlaps some log latency windows; its overhead
prevents treating those windows as an uninstrumented A/B.

The focused benchmark retains an old reader snapshot with 8,192 routes and
compares zero-key and 64-key retirement publication, 1,000 iterations per mode.
The exact old full-rebuild function is retained only under `cfg(test)`.
The measured boundary is enqueue of a following private message through the
publication branch to dequeue. Queue capacity/high-water are 2 and overflow is
0. Initial population, actual owner-map removal and snapshot restoration are
outside the measurement; this measures publication cost, not the entire GC
coordinator, route-index cleanup or private trade application.

Run `cargo test --release -p hexagent-exchange
benchmark_retirement_snapshot_publication -- --ignored --nocapture
--test-threads=1`. Tests separately cover duplicate keys sharing a shard,
immutable old readers, unrelated event isolation, coherent three-map removal,
repeated commands and subsequent replay ownership.

Local x86_64 macOS, rustc 1.97.1, release/fat LTO, one codegen unit, native CPU,
System allocator. Nanoseconds; each row has N=1,000, queue peak/capacity=2,
overflow=0:

| Targets / publication | P50 | P99 | P999 | Max |
|---|---:|---:|---:|---:|
| 0 / old full rebuild | 4,178,175 | 5,834,617 | 6,976,915 | 7,401,428 |
| 0 / unchanged branch | 60 | 71 | 80 | 246 |
| 64 / old full rebuild | 3,953,395 | 5,076,489 | 5,752,142 | 7,301,680 |
| 64 / changed shards | 898,398 | 1,248,996 | 1,683,971 | 1,897,068 |

The near-zero row measures only skipping publication; it does not imply that
the entire GC turn or a live private message has nanosecond latency. Old-reader
retention and reset costs are outside the measured section in both modes.
The exchange release suite passed 627 tests (0 failed, 11 ignored), and the
engine release check passed. Production validation must additionally confirm
GC progress, mirror watermarks and downstream queues across event rollovers.

## Remaining migration

Affected shards still allocate/copy on retirement; a large batch touching all
shards is not allocation-free. Cold full-state reconciliation/checkpoints and
the account's separate ShardedRouteMap/history deletion path remain separate
migrations. Their owner-apply cost must be measured independently of the GC
coordinator and execution identity publication. No durability weakening,
allocator purge disabling or private-event loss is used to reduce latency.
