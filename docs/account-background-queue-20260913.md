# Account background queue investigation (2026-09-13)

Settled-history GC shared the private lifecycle worker and could park it on
the cold account's control/state locks. An independent runtime ownership
cleanup also executed an ArcSwap RCU update on every one of 65,536 slots,
including empty and unrelated slots. This change makes the two GC lock
acquisitions defer work when busy and filters runtime ownership slots before
RCU publication. Private event order, ownership and risk checks are unchanged.

## Production evidence and limits

The pre-change release used SDK `d7fc5c5` and hexbot `6bca114`. Log analysis
covers 10:10:28–11:40:43 UTC. The lifecycle enqueue-to-apply-start stage had
6,203 events and a maximum of 150.75 ms. The worst one-minute window ending
11:05:30 had 70 events, median 6.7 us, P99/P999/max 150.75 ms. The corresponding
fast-route-to-owner maximum was 152.87 ms. These are minute histograms; their
quantiles cannot be pooled into a whole-period percentile.

Seven minutes of bounded production perf recording (11:43:08–11:50:08 UTC)
captured 838 CPU samples from four account workers. The cold owner contributed
391 samples, with 223 stacks through lifecycle mirror reconciliation and 42
through full persistence scheduling/state cloning. The lifecycle owner
contributed 80 samples, with 30 through runtime ownership bulk retirement / RCU
reader debt payment. Stack counts overlap and are not independent percentages.
The historical 150 ms queue event did not recur during this perf window, so the
exact historical stall is not attributed to a sampled stack. Cold-lock blocking
is independently reproduced by the regression test below.

The writer made 1,902 fdatasync calls: median 1.827 ms, P99 2.268 ms, P999
3.873 ms, max 4.029 ms. No account owner fdatasync occurred in this capture.
Lifecycle-thread allocator madvise calls reached 435 us under shard-map cloning.
Trace overhead is included. These observations do not prove that storage never
stalls outside the capture window.

`snapshot_owner_roundtrip` is the chain poller's wallet calibration request and
reply, not a monitoring snapshot or a pure queue timer. Its 248.73 ms historical
maximum includes the required 50 ms private lifecycle grace, watermark catch-up,
owner scheduling, snapshot application and reply. The new
`wallet_calibration.wait_including_grace` measures the age of the selected
coalesced payload at claim time; `wallet_calibration.apply` measures its apply
and result construction. Neither changes the grace or watermark checks.

## Ownership, backpressure and recovery

- The cold owner still owns aggregate state; strategy threads retain their own
  mutable StrategyAccount. The added request timestamp is immutable after
  publication. Older coalesced generations preserve the selected timestamp.
- No workers, queues or priority changes are introduced. Existing topology and
  affinity assignments remain applicable. Private lifecycle queues stay bounded
  and lossless; GC runs after lifecycle and maintenance work.
- Busy candidate validation preserves the candidate. Busy final commit preserves
  the certified inflight generation. The next coalesced wake or 100 ms idle poll
  revalidates eligibility, membership and epochs. No retry sleeps or synchronous
  requests are added. GC progress may defer while the cold owner remains busy.
- Runtime bulk retirement still scans a fixed-capacity table. Only matching
  entries publish a replacement. The RCU closure rechecks identity so a concurrent
  replacement is not incorrectly erased. Reader snapshots remain valid.
- New latency stages are prepared on their owner thread before the startup
  barrier. GC busy samples carry 1 ns; their event count is the contention count,
  not a measured lock-wait duration. They use the existing asynchronous recorder.
- The periodic `account_queue_metric` log runs on the existing monitoring
  worker. `lifecycle_commands` covers SharedAccount commands; it is distinct from
  the exchange's AccountLifecycleJob lane. Mirror depth/high-water/overflow,
  published/applied watermarks, and persistence backlog are also exposed.
  High-water/overflow counters are process-cumulative; depth is instantaneous.

## Focused before/after measurement

Local x86_64 macOS, rustc 1.97.1, release optimization, fat LTO, one codegen unit,
system allocator. Quanta startup calibration is excluded. This is a blocking
reproduction, not an estimate of Linux production latency.

For each phase, 1,000 fixtures inject a 2 ms cold control-lock hold while a
single consumer receives GC followed by a private message in a bounded queue.
The boundary is private enqueue to dequeue after the GC turn. Capacity and
peak depth are both 2; overflow is 0 before and after. Setup and cold-thread
join are outside the measured section. Nanoseconds below:

| Phase / version | N | P50 | P99 | P999 | Max |
|---|---:|---:|---:|---:|---:|
| Candidate / before | 1,000 | 2,114,105 | 3,092,506 | 3,130,259 | 3,171,545 |
| Candidate / after | 1,000 | 6,395 | 21,499 | 29,463 | 37,150 |
| Commit / before | 1,000 | 2,118,722 | 3,093,589 | 3,120,567 | 3,123,223 |
| Commit / after | 1,000 | 10,735 | 27,331 | 50,458 | 82,090 |

The old candidate regression fails while the cold lock is held; both candidate
and certified-commit regressions now return without waiting on control/state
locks, retain work, and finish exactly once after release. Account release
tests passed: 242 passed, 0 failed, 6 ignored, including existing stale
certificate, re-registration, overflow, isolation and replay checks.

Reproduce with `cargo test --release -p hexagent-account
benchmark_gc_cold_contention_private_queue -- --ignored --nocapture
--test-threads=1`. Runtime retirement has a separate ignored benchmark,
`benchmark_runtime_ownership_bulk_retirement`, which executes the exact old
RCU loop and new filter in the same binary, with 64 target and 1,024 unrelated
routes. Each mode measures 1,000 complete bulk-retirement calls; insertion is
outside the measurement and there is no queue in that benchmark.

| Runtime retirement / version | N | P50 ns | P99 ns | P999 ns | Max ns |
|---|---:|---:|---:|---:|---:|
| All-slot RCU / before | 1,000 | 10,459,879 | 21,119,824 | 24,673,579 | 28,242,130 |
| Read-filtered RCU / after | 1,000 | 1,724,676 | 2,087,294 | 3,068,427 | 3,155,076 |

The exchange release suite passed: 624 passed, 0 failed, 10 ignored. Its
functional cases include private owner ordering, bounded overflow, recovery,
and route retirement with retained reader snapshots and later replay routes.
The full fixed-capacity read scan remains visible in the after measurement.

## Remaining incremental migration

This change removes two cold-lock waits, not all legacy locks or allocations.
Cold lifecycle mirroring still performs full reconciliation and publishes
snapshots under the control/state locks. Checkpoint and maintenance paths still
clone full persisted state; the writer serializes/diffs that state. A follow-up
should move those paths to typed persistence records and reconcile only touched
economic scopes while retaining pending-physical and ownership checks.

Account route shards still clone immutable hash maps when edited. Bounded
settled-history batches can scan accumulated history and trigger allocator
purging. A per-owner retirement index and bounded generation reclamation need
separate replay/idempotence/overflow measurements. No purge disabling, weakened
durability or relaxed private-event correctness is included here.

Production validation must check lifecycle queue and route-to-owner latency,
GC certificate progress, mirror lag and persistence backlog across event
rollovers. A short quiet window cannot establish disappearance of a rare
150 ms tail or a whole-run P99/P999 improvement.
