# CLOB canonical snapshot cache: production stack and buffer reuse

The maker02 frame at 2026-09-15 09:54:18.235 UTC spent 24,283 us in
`price_change_apply` (24,295 us handler, 1 us JSON decode). Its existing
99 Hz cycles pre-trigger capture
`clob-price-change-prewindow-tid-21169-1789463407.data` ends with three
consecutive samples at monotonic 159720.062543038, 159720.070121815 and
159720.078555684:

```text
process_clob_frame_in_place -> ClobLocalBooks::apply_price_change
 -> ClobLocalBooks::canonicalize_token -> OrderBookSnapshot::clone
 -> mi_malloc_generic -> mi_theap_collect_ex -> mi_arenas_collect
 -> mi_arena_purge -> mi_os_purge_ex -> madvise
 -> kernel page unmap/free
```

The 16.013 ms span across these samples is direct evidence of allocator
purging on the live CLOB owner. It is not a wall-time attribution obtained by
adding CPU sample periods. The captured 605-byte frame, containing two top
level deletes, is retained as the regression fixture. The deployed SDK was
`9bb6ebc96a0c6b8757f33c40a209690381a09461`.

The redundant cache clone is replaced by copies into owner-local buffers
allocated when subscriptions are initialized. Each canonical condition
reserves 256 levels per side, matching the full-book wire schema's capacity.
The cache remains unpublished until the first accepted canonical version.
Outgoing snapshots continue to own independent vectors. Later updates cannot
change events already queued to consumers. Older complementary seeds still
re-emit the newest accepted canonical snapshot.

No new worker, queue, lock or cross-thread state is introduced. The CLOB
owner remains the sole writer. Existing subscription and event delivery
semantics are unchanged. Deltas can accumulate more than 256 levels, so that
case retains complete depth and the existing Vec growth path; no truncation
or unsupported depth limit is introduced.

## Focused evidence

Local macOS debug builds, same process for alternating cache-maintenance A/B,
20,000 operations per implementation and depth. Boundary: cache update,
including destruction of the previous cache in the clone/replace baseline.
Both implementations are warmed, and execution order alternates within each
pair. Queues are absent: queue high-water 0, overflow 0. Times below are ns.

| Depth/side | Mode | Allocations | P50 | P99 | P999 | Max |
|---:|---|---:|---:|---:|---:|---:|
| 1 | clone/replace | 60,000 | 468 | 764 | 843 | 22,694 |
| 1 | reuse | 0 | 272 | 434 | 559 | 2,514 |
| 16 | clone/replace | 60,000 | 987 | 1,561 | 4,508 | 27,264 |
| 16 | reuse | 0 | 476 | 795 | 884 | 22,025 |
| 256 | clone/replace | 60,000 | 9,154 | 13,616 | 33,014 | 81,116 |
| 256 | reuse | 0 | 3,531 | 5,402 | 21,088 | 28,196 |

The full-path replay uses the exact captured frame, a synthetic compatible
seed book, and alternating reinsertion to make every iteration change the
BBO. The complete live book at capture time was not recorded. The measured
boundary is resident parsing, full BBO-change application and batch drop;
10,000 frames per run, queue high-water 0, overflow 0:

| Version | Allocations | Allocated bytes | P50 ns | P99 ns | P999 ns | Max ns |
|---|---:|---:|---:|---:|---:|---:|
| before | 420,000 | 56,790,000 | 114,059 | 235,517 | 313,253 | 353,612 |
| after | 320,000 | 51,830,000 | 142,618 | 147,653 | 200,908 | 267,121 |

The sequential full-path debug timings overlapped other local compilation;
median worsened while upper percentiles improved, so they do not establish a
production latency improvement. The allocation reduction is 23.8%, from 42
to 32 allocations per frame. The controlled A/B isolates the removed cache
allocation mechanism. Production P99/P999 must be evaluated after deployment.

## Validation and remaining work

Four focused tests cover zero-allocation cache refresh within reserved
capacity, complete depth above capacity, immutable previously emitted
snapshots, newest canonical version and complementary seeding, and withholding
unseeded startup buffers. Existing market tests cover BBO repair, quarantine,
sequence ordering, duplicates, failover and bounded event delivery.

Run the regular tests and focused benchmarks with:

```sh
cargo test -p hexagent-exchange exchange::polymarket::market:: --lib -- --test-threads=4
cargo test -p hexagent-exchange benchmark_captured_clob_top_change --lib -- --ignored --nocapture --test-threads=1
cargo test -p hexagent-exchange benchmark_canonical_cache_maintenance_ab --lib -- --ignored --nocapture --test-threads=1
```

This change removes one proven allocator entry point. Ten of eleven existing
captures had a madvise stack in their final 30 ms; other callers included
price-change temporary containers, snapshot construction, event coalescing
and health publication. Emitted snapshot allocation, temporary collections,
rare oversized cache growth and compatibility with dynamically installed
roles remain incremental migration work. This is not an allocation-free
full CLOB path or proof that all production tails are removed.
