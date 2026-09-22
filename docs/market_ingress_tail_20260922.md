# Market ingress and book-allocation tails (maker02, 2026-09-21)

The investigation covers the normal 17:35:24.866 UTC boot through
2026-09-22 02:05:00.070 UTC. Hexbot's companion report is
`docs/live_record_root_causes_20260922.md`.

Two quote signals had parser-receipt ages of 118 ms and 828 ms, while the
router-to-worker queue and callback histograms stayed below a millisecond.
The original queue timer begins at router publication, so it cannot observe
earlier feed/root backlog. A healthy 10-second heartbeat sample also loses a
stall that has already recovered.

* Record parser local receive time to root routing and strategy callback entry,
  separately from the existing router-to-worker queue timer. Exclude scheduled
  control events, historical bars, missing and future receive timestamps.
  These ages use the source's wall-clock domain, not exchange timestamps.
* The receiver's sole owner retains maximum poll gap and pending depth in
  preallocated scalar atomics. The existing background supervisor formats them.
  Reset the measurement once after startup/replay drain. A poll gap includes
  idle waits; it is evidence of an interval, not proof of a particular lock.
* Prewarm the two metric stages before each owner enters its live loop. No new
  threads, queues, ownership routes, or hot-path formatting are introduced.

The retained CLOB perf capture ending at 22:30:19 UTC identifies a concrete
allocation tail: `ClobLocalBooks::apply_book -> RawVec::grow_one -> mimalloc
collect/purge -> madvise -> zap_pte_range`. The trigger is a 70-bid/29-ask book;
book apply took 15.436 ms, while JSON parsing took 12 us. Parse-only replay does
not reproduce the order-book allocator path.

The temporary full-book output now has exactly two inline slots (one canonical
snapshot and at most one health transition). Snapshot sides allocate their
known depth once instead of repeatedly growing through `Option<Vec<_>>::collect`.
Public events retain independent ownership; later updates cannot mutate a
queued snapshot, and reconnect/repair/version ordering remains unchanged.

The migration is deliberately incomplete: BTreeMap nodes, strings, frame
batches and the event-owned depth buffers still allocate. Removing those safely
requires preallocated subscription storage plus a bounded snapshot ownership
and return protocol. Do not replace them with shared mutable book references.

Tests cover retained maxima after recovery, receiver isolation, ordered and
replaceable queue overflow/reconnect, missing/future timestamp handling, exact
snapshot depth/mirroring/ownership and allocation counts. The new focused
benchmarks print N, P50/P99/P999/max, allocation count, queue depth and overflow:

```sh
cargo test -p hexagent-engine --lib
cargo test -p hexagent-exchange --lib
cargo test -p hexagent-exchange --lib --release market_mailbox_consumer_latency_benchmark -- --ignored --nocapture
cargo test -p hexagent-exchange --lib --release benchmark_clob_live_snapshot_depth -- --ignored --nocapture
```

These local microbenchmarks are not an end-to-end production latency claim.
The original router pause still needs a matching stack/scheduler capture;
additional age instrumentation must not be presented as removing that pause.

Local optimized benchmarks (x86_64 macOS, release opt-level 3 / fat LTO,
100,000 events each; ns):

| Boundary | P50 | P99 | P999 | max | Allocations/event |
| --- | ---: | ---: | ---: | ---: | ---: |
| Previous 70-bid/29-ask snapshot conversion | 5,633 | 6,766 | 21,139 | 104,522 | 10 |
| Exact-capacity snapshot conversion | 4,197 | 4,457 | 6,517 | 58,334 | 2 |
| Instrumented mailbox try_recv entry to return | 96 | 103 | 112 | 13,900 | 0 |

Snapshot bytes allocated per event fell from 4,992 to 1,584. These are owned
output buffers, not dropped depth. The temporary two-event result allocates
nothing. Snapshot benchmark queue depth/overflow = 0/0 (no queue); mailbox
pending/high-water/contention-drops = 0/1/0.
