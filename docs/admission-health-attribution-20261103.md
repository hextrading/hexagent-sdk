# Execution health attribution on maker02

The 2026-11-03 13:07:01 UTC `Degraded` snapshot was a new CLOB slow
response, not a state left behind by the earlier recovery. Fast slot 0 took
1,015.464 ms, crossing the unchanged 500 ms business threshold. Cancel slot 1
then took 1,005.858 ms. Fast slot 0 was retired after another slow response,
and its replacement became business-verified at 13:07:06.182. Cancel slot 1
remained failed until a real healthy business response at 13:07:40.082, when
the account returned to `Healthy`.

Of 150 observed Degraded snapshots from the process started at 12:20:23 UTC
through 13:51 UTC, 104 had an unready ordinary Cancel lane, 18 had an
unverified Fast lane, and 28 had both. None was unexplained by those
predicates. These are emitted snapshots, not a census of every internal
transition. The evidence does not locate the upstream CLOB delay internally.

## Diagnostic change

`reason=open` means capacity is available. It does not mean every lane is
healthy. Keep that capacity field and add a separate `health_reason` and
`health_blockers` bitset derived from the existing Healthy predicate:

| Bit | Blocker |
| --- | --- |
| 1 | Fast owner lease stale |
| 2 | Fast transport unready |
| 4 | Fast business evidence unverified |
| 8 | Ordinary Cancel owner lease stale |
| 16 | Ordinary Cancel transport unready |
| 32 | Ordinary Cancel business failed |
| 64 | Reserved Cancel owner lease stale |

Zero means these lane predicates are satisfied; account recovery/backoff and
available capacity remain described by the existing `blockers` and `reason`.
Multiple bits produce `health_reason=multiple`. `unverified_fast_mask`,
`failed_fast_mask`, `failed_cancel_mask`, and `unready_cancel_mask` identify
physical slots (first 64 only); aggregate flags include every slot. Reserved
Cancel transport readiness is intentionally excluded, exactly as in the
existing Healthy predicate. Busy Fast lanes affect capacity, not health.
Lane evidence now includes the existing `business_elapsed_ns` observation.

No health threshold, deadline, routing rule, state transition or recovery
success requirement changes. GET prewarm, timers, duplicate results and old
generation responses still cannot substitute for fresh business evidence.

## Ownership and verification

The account transport router remains the sole owner of lane health. New fields
are fixed-size derived values in an immutable diagnostic copy, with no new
runtime authority or strategy-account access. The existing 1,024-record bounded
diagnostic channel retains producer ordering and explicit overflow accounting;
diagnostic loss never changes private lifecycle delivery or health authority.
Formatting and
I/O stay on the existing affinity-managed `execution-diagnostics` worker.
No new workers, queues, locks, or heap-growing operations are introduced.
Private lifecycle and strategy-owner delivery paths are unchanged. This does
not extend the legacy account aggregation authority into strategy risk state.

Regression cases replay overlapping Fast/Cancel slowness, generation retirement
and replacement, duplicate result rejection, the exact 500 ms boundary,
reserved-owner lease expiry and account isolation. Existing ordering, queue,
probation and recovery tests remain required.

Run native release tests and the focused benchmark:

```
cargo test --release -p hexagent-engine execution_admission::tests --lib -- --include-ignored --nocapture
cargo test --release -p hexagent-engine admission --lib
cargo test --release -p hexagent-engine compact_publication_diagnostic --lib
```

The benchmark measures only immutable `capacity_detail` derivation over
alternating healthy/degraded four-Fast/four-Cancel accounts, after warmup,
200,000 samples. There is no queue in this boundary. Native before/after
results and live stage tails/queue counters belong in the Hexbot deployment
evidence; this microbenchmark alone is not an end-to-end latency claim.

Native maker02 release results (CPU 0–1, SCHED_OTHER nice 19): 45 admission
and routing tests passed, plus the diagnostic-copy test; all four focused
admission benchmarks also passed. Before/after immutable-copy measurements:

| Version | Samples | Median | P99 | P999 | Maximum | Depth / overflow |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Before | 200,000 | 59 ns | 63 ns | 152 ns | 373,646 ns | 0 / 0 |
| After | 200,000 | 66 ns | 69 ns | 78 ns | 39,977 ns | 0 / 0 |

The diagnostic detail grows from 88 to 128 bytes. This derivation runs for
transition/periodic diagnostics, not on every quote. Background scheduling
affects the maxima; these runs do not establish an end-to-end improvement.
