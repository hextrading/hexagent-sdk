# Deferred reclamation of GC route snapshots

The lifecycle owner previously dropped the old route table returned by each
successful GC `ArcSwap::rcu` publication. If a private lookup still held an
ArcSwap `load()` guard, the final reader instead inherited that table's
destructor. Moving old snapshots to the existing cold account owner controls
which lane performs the destruction. This does not remove route-table cloning
from publication and is not a claim that every publisher percentile improves.

## Ownership, priority, and bounds

- The private lifecycle owner applies an authorized GC mutation under the
  existing account control gate. It reserves a retirement credit **before**
  deleting ledger state or preparing its WAL delta, then transfers old immutable
  route tables into that credit's fixed batch. The new table remains published.
- The sole consumer is the existing non-cloneable `SharedAccountOwnerState` on
  the already pinned cold account worker. Its `RefCell` holds at most one pending
  batch. No new thread, scheduling class, or topology role is added.
- Reclamation runs on the cold worker's existing 10 ms tick, below private
  lifecycle-mirror, control-command, and wallet wake priority. A busy higher
  priority lane can defer it; it never blocks those lanes waiting for a reader.
- Eight credits cover **all** issued permits, queued batches, and the batch
  already removed by the consumer but still held by readers. A credit returns
  only after every old table in its batch is reclaimed. The channel also has
  eight preallocated slots, so each issued permit has reserved send capacity.
- Each batch holds at most 32 table references: two route maps per order and two
  per trade, with at most eight retired orders and eight retired trades per GC
  turn. A successful shard publication returns only one old table, independent
  of RCU retry count. Repeated no-op updates do not retain the current table.
- These are bounds on retained table count, **not constant byte bounds**. Bucket
  memory scales with shard capacity; immutable key/owner `Arc<str>` bytes remain
  shared. There are at most 256 retained old tables across eight full batches.
- A full credit budget returns `Busy` before mutation. The existing GC mailbox
  retains the same request and retries later. No lifecycle event, reservation,
  completion certificate, or WAL generation is dropped to relieve pressure.
  The retirement sender uses `try_send`; its capacity invariant is asserted.

The cold owner checks `Arc::strong_count == 1` before dropping a retired table.
ArcSwap 1.9.2 pays outstanding reader debts inside the successful CAS before
`rcu` returns the old Arc. Thus a still-live `load()` guard is represented in
the strong count by reclamation time. New readers see the replacement table;
the route API does not publish weak references to retired tables. Only the cold
owner can be the last strong owner when it performs the destructor. A stalled
reader can hold the first pending batch and eventually fill all eight credits;
the resulting bounded backpressure is intentional.

`route_retirement_metrics()` reports outstanding credits, high-water,
backpressure attempts, and reclaimed batches. Latency stages separately report
`polymarket.account.route_reclaim.cold_drop` and completed
`polymarket.account.route_reclaim.retirement_age`. The age is measured from
permit creation, including publication and retirement waiting; it is not pure
queue residence. An indefinitely held reader has no completed age sample, so
outstanding/backpressure must also be monitored.

## Correctness validation

Six focused tests cover bounded credits including a reader-held pending batch,
owner rebinding and stale-owner removal, independent accounts and unused-credit
return, production `load()` guard lifetime, no-op publication without retained
current snapshots, and persistent full-credit GC backpressure.

The last test starts a real account WAL writer and an eligible deletion, fills
all eight credits, and has the cold owner retain one reader-held batch. `Busy`
leaves the complete durable account, virtual order/trade rows, scheduled WAL
generation, and WAL bytes unchanged. Releasing the readers permits the same
request to retire orders/trades and schedule a new WAL generation. All six
tests passed. Existing route tests also cover concurrent owner updates and
immutable snapshots.

## Publication benchmark, without a held reader

```sh
cargo test --release -p hexagent-account \
  route_retirement_successful_gc_publication_benchmark -- --ignored --nocapture
```

One release binary compares inline and deferred old-table destruction. Both
start with 50,000 realistic-length retired-trade keys distributed over the
production 64 shards. After 64 warmup GC events, 1,000 measured events each add
eight route rows, growing history from 50,512 to 58,512. Input strings and
initial seeding are outside timing. The publication boundary includes credit
reservation (deferred mode), grouping, table clone, successful RCU publication,
and enqueue or synchronous old-table destruction. There are no concurrent RCU
writers or held readers in this case. The deferred variant drains eight queued
batches at a time outside the measured publication interval.

Local run: 2026-09-15, rustc 1.97.1, x86_64 macOS, Intel i9-9880H 2.30 GHz,
workspace release profile with fat LTO and one codegen unit. The workstation is
unpinned and shared with development work. Values below are microseconds;
each row has 1,000 individual observations with nearest-rank percentiles.

| Boundary | P50 | P99 | P999 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| GC publication, inline destructor | 178.230 | 295.241 | 365.261 | 460.178 |
| GC publication, deferred destructor | 189.909 | 301.926 | 346.229 | 385.311 |
| Deferred cold destructor | 118.270 | 188.488 | 250.890 | 305.114 |
| Completed retirement age | 1,393.649 | 1,933.234 | 2,119.745 | 2,132.980 |

The deferred run ended with outstanding 0, high-water 8, backpressure 0,
overflow 0, and 1,064 reclaimed batches including warmup. The inline run has no
retirement queue. **This case did not improve publisher median or P99.**
Retaining multiple old tables changes cache and allocator reuse, which can
offset the removed immediate destructor cost; that explanation is an inference,
not a measured allocation profile. The cold cost remains real and is shown
separately. The motivating protection is the lane on which a final-reader
destructor runs.

## Last-reader release benchmark

```sh
cargo test --release -p hexagent-account \
  route_retirement_last_reader_drop_benchmark -- --ignored --nocapture
```

The second case gives the affected production route shard exactly 3,000
realistic-length keys and holds one actual ArcSwap `load()` guard across a
successful owner rebind. It isolates a private lookup's one-shard lifetime;
unrelated shards are outside the measured boundary. This is a larger affected
shard than the approximately 800–900 rows in the first whole-map case.

Both variants execute in the same binary, alternating which runs first for
each event. After 64 warmup events, each has 1,000 measured events. The benchmark
asserts that after publication the guard is the sole old-table strong owner in
the inline variant, versus two strong owners (guard and retirement batch) in
the deferred variant. A cold probe while the guard is held must retain that
batch. It then times dropping the guard, followed by a separately timed cold
reclamation after the guard is gone.

Each ownership phase runs sequentially on the benchmark thread; this isolates
destructor placement and does not simulate scheduler wakes or network traffic.
The cold phase runs immediately, rather than waiting for the production 10 ms
tick. In particular, its retirement-age distribution is not a production queue
latency estimate. The host, compiler, release profile, percentile definition,
and unpinned measurement limitations are the same as above.

All values are microseconds; N=1,000 for each row:

| Boundary | P50 | P99 | P999 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Publication with held reader, inline | 49.748 | 84.344 | 103.119 | 103.550 |
| Publication with held reader, deferred | 49.855 | 87.077 | 139.642 | 140.396 |
| Last reader release, inline | 45.160 | 95.928 | 112.773 | 114.044 |
| Last reader release, deferred | 0.039 | 0.040 | 0.041 | 0.041 |
| Cold probe while reader held | 0.191 | 0.217 | 0.224 | 19.765 |
| Cold destructor after reader release | 45.472 | 80.849 | 107.542 | 107.620 |
| Completed retirement age | 95.634 | 149.431 | 200.842 | 201.269 |

The deferred case finished with outstanding 0, high-water 1, backpressure 0,
overflow 0, and 1,064 reclaimed batches including warmup. The inline case has
no retirement queue. This case demonstrates removal of the table destructor
from the last reader: reader-release P99 falls from 95.928 microseconds to
0.040 microseconds. The approximately 45-microsecond median destruction cost
still exists on the cold phase. Publisher tails do not improve in this run;
do not add percentile columns across phases or interpret the reader result as
an end-to-end trading latency improvement.

## Remaining scope and production verification

Only old tables from successful GC replacement publications enter this queue.
Failed RCU candidates still drop their cloned table on the publishing thread.
Non-GC `insert`, `remove`, and cold aggregate `publish_owners` still use their
existing drop behavior. Route grouping and table cloning also remain in the
existing lifecycle GC turn. A no-op returns no retirement batch; a concurrent
non-GC replacement can still leave that caller as the last temporary reader.
This change does not establish a universal guarantee for all route readers.

The incremental migration is to profile remaining clone/retry and non-GC
destructor stacks after deployment, then transfer additional retired immutable
tables through bounded owner-controlled lanes or replace whole-shard cloning
with a suitable owner-published representation. Do not add an unbounded garbage
queue or synchronous wait in the private lane. Any extension must reserve its
own explicit capacity before mutation and preserve conditional ownership checks.

Production verification must include private lifecycle queue P99/P999/max,
quote/market callback tails, GC publication and cold-drop stages, retirement
outstanding/high-water/backpressure, cold account command/mirror backlog, and
completed retirement age. Benchmarks isolate ownership phases and do not prove
end-to-end latency gains or that cold queues cannot accumulate under load.
