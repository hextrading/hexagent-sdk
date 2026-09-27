# maker02 ledger and observation follow-up — 2026-09-27

## Evidence and scope

The live process started at 11:20:22 UTC and still ran SDK `17058da` when a
fixed window ending 12:44:00 was downloaded. The 83m38s export contains 6,416
placements, 300 positive private fill increments and 600 zero-increment
replays, with no duplicate positive increments or order audit errors. All
three accounts have 166 reconciliation samples with zero cash/position
residuals. The four placement orphans are the already investigated 11:45
network incident; no new placement timeouts occurred afterward.

Two continuing local problems are measurable:

- At 11:59:28 the CLOB observation FIFO reported high-water 65,536 and 11,272
  dropped samples. Draining only at the 60s report interval limits sustained
  capacity to roughly 1,092 observations/s. These are advisory latency samples,
  not lost order/private events, but their loss biases the busy-minute figures.
- Registration ledger had N=6,307, median minute P50/P99/P999 41/577.9/726.5 μs,
  max 21,640 μs. The quote calculation had N=150,353 and corresponding
  14.3/24.6/30.7 μs, max 59 μs. Minute quantiles are not pooled event quantiles.
  The remaining account route index copied every Arc entry in one of 64 shards
  for each fresh route. This was separate from the execution index fixed earlier.

HTTP remains a separate limitation: 138 requests exceeded 500ms, all reused a
connection and had valid TCP_INFO, with retransmissions observed on only 3.
Median write/flush-to-first-read fraction was 99.9965%. That boundary includes
remote processing, network and local wake/poll scheduling; it cannot identify
the remote root cause. This change does not alter HTTP admission or retry rules.

## Changes and ownership

1. The existing affinity-managed `latency-dump` worker drains observation FIFOs
   every 250ms, independently of the configured report interval. Each pass is
   bounded by the depth observed at entry. Binning, formatting and export remain
   on that worker. Producer publication, FIFO capacity 65,536 and drop-new with
   an overflow counter are unchanged. The reported `drained` count now includes
   every intermediate pass since the preceding export. Timing windows are
   consumption windows, not exact event timestamp partitions.
2. Each account route shard publishes 64 shared immutable leaves. Point updates
   copy 64 Arc pointers and the affected leaf, rather than the whole historical
   shard. Outer RCU retries, conditional owner removal, old-reader immutability
   and the existing eight-credit GC retirement lane remain in place. Mutation
   stays with the existing lifecycle/cold ownership lanes; no quote-path shared
   account admission or new global index is introduced. Lookup reuses the hash
   that selected the outer shard. Route restoration remains ledger-derived and
   adds no persistent format.
3. Fresh successful registrations enqueue compact observations for access/
   validation, reservation mutation, route publication and mirror/WAL enqueue.
   Linux thread CPU and wall-minus-CPU are recorded across registration through
   persistence enqueue, excluding the final return clone. Off-CPU is an estimate
   including descheduling and blocking; it is not a lock-only measurement.
   Unsupported CPU clocks produce no CPU/off-CPU samples. Startup prewarms the
   six stages and the owner-local observation queue. No new thread, lock, I/O,
   histogram aggregation or dynamic stage registration enters quote processing.

The route optimization reduces background work. It does not prove that copying
caused the entire 21.64ms wall-time outlier. The added phase/CPU observations are
needed to resolve that remaining attribution question in the next live window.

## WAL replay defect found during regression

An existing test intermittently failed with `WAL generation 7 has no prior
numeric value for instances/owner/cash`. Its saved frame contains startup
instance/seed creation followed by the first trade. The historical stale-snapshot
repair detector compared its direct balance leaves against the empty pre-frame
state, despite the same checksummed frame establishing those balances earlier.

A deterministic regression reproduced the failure before the repair. Frames
that replace the state root, seed authority, instance roots or position maps
now do not qualify as evidence for this particular stale-publication repair.
They replay normally and still pass the full economic validator. The regression
reopens the valid coalesced frame twice and confirms that a separate unexplained
one-unit cash discrepancy is rejected. Existing narrowly proven historical
repairs retain their tests. This defect was found in tests, not as a new live
inventory discrepancy in the downloaded window.

## Focused measurements

macOS release, LTO off, 16 codegen units. Same-process flat deployed Arc-byte
shards versus leaf snapshots, alternating update order, 45,000 retained routes
and N=5,000 additional inserts. Boundaries include immutable publication and
release of replaced snapshots; row input preparation is outside the timer.
Synchronous microbenchmarks have queue depth/overflow zero.

| Boundary | N | P50 ns | P99 ns | P999 ns | Max ns |
|---|---:|---:|---:|---:|---:|
| Flat shard insert | 5,000 | 26,317 | 60,660 | 87,949 | 108,049 |
| Leaf snapshot insert | 5,000 | 2,116 | 4,746 | 18,842 | 32,078 |
| Flat shard lookup | 5,000 | 254 | 490 | 737 | 15,483 |
| Leaf snapshot lookup | 5,000 | 277 | 631 | 911 | 21,861 |

The extra indirection has a small lookup cost in this fixture; it is not a claim
that every latency boundary improves. Live end-to-end and private application
latencies must be checked alongside registration and queue high-water.

Observation cadence benchmark: 120,000 inputs representing a logical 60s at
2,000/s in 500-event bursts. This simulates drain cadence without sleeping 60s;
it is not a wall-clock throughput claim. Producer measurements exclude drain
work; the report-only producer includes failed publications when full.
Both schedules use the new drain function, isolating cadence/capacity rather
than comparing the old consumer's histogram implementation.

| Boundary | N | P50 ns | P99 ns | P999 ns | Max ns |
|---|---:|---:|---:|---:|---:|
| Report-only producer | 120,000 | 44 | 47 | 102 | 27,351 |
| Frequent-drain producer | 120,000 | 45 | 49 | 100 | 29,994 |
| Report-only drain | 1 | 1,008,902 | 1,008,902 | 1,008,902 | 1,008,902 |
| Frequent drain pass | 240 | 7,543 | 27,959 | 34,921 | 34,921 |

Report-only high-water/drop/drained: 65,536 / 54,464 / 65,536. Frequent drain:
500 / 0 / 120,000. Final depth is zero for both. N=1 has no meaningful tail
estimate. Producer algorithm and capacity are identical in both cases.

The retained GC credit/reclamation benchmark also passed after changing the
snapshot representation. With 50,512–58,512 routes and N=1,000 batches of eight
changes, deferred publication P50/P99/P999/max was
14.892/44.433/57.292/81.641 μs; cold destruction was
7.136/27.618/34.033/35.231 μs and retirement age was
107.649/216.070/234.054/238.456 μs. Outstanding ended at zero, high-water eight,
backpressure/overflow zero. A held-reader fixture with 3,000 rows in its shard
had last-reader drop 42/71/85/2,158 ns (N=1,000), with the old generation retained
for cold reclamation. These are current-implementation GC checks, not an extra
before/after claim about the leaf optimization.

## Validation and migration boundary

- Release suites: account 348 passed, runtime 79 passed, exchange 930 passed,
  engine 147 passed. Account/runtime were rerun after the lookup hash adjustment.
- Tests cover shared old snapshots, concurrent owner updates, rebound ownership,
  idempotence, private-before-registration ordering, replay, saturated retirement
  before mutation, separate-account credits, telemetry FIFO overflow/recovery,
  frequent-drain sample preservation and prompt unified shutdown.
- The legacy account route RCU writers and virtual-account access still exist
  off quote processing; this change reduces their publication cost rather than
  adding new shared-authority uses. A full migration must move remaining cold
  route updates behind owner messages with equivalent replay and GC evidence.
- Live rollout evidence is recorded in hexbot. Raw fixed-prefix exports, test
  failure/reproduction output and benchmark logs are retained locally under
  `docs/evidence/maker02_followup_20260927T1244Z/` in the hexbot checkout.
