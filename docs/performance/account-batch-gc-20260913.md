# Account GC and HTTP repair — 2026-09-13

A failed HTTP repair probe previously returned a quarantined slot to business admission. Both transport implementations now retain quarantine across failed status, truncated-body and timeout probes; one existing order-runtime task retries per slot with a 500 ms probe deadline and 100 ms–5 s backoff. A candidate is installed only after a successful, fully drained response. Stale repair completions cannot release a newer quarantine. No extra connection slot, worker or business-request retry is introduced.

GC now maintains derived per-instance token and trade-to-order indexes. Owner turns select at most 128 order keys, 128 trade keys and 128 trade protection checks, retiring at most eight orders and eight trades. Rotating cursors survive cold snapshot publication. The completion certificate distinguishes an unfinished sweep from protected history; incomplete zero-deletion turns continue, while a completed protected sweep waits for private activity. Existing epoch, registration and durable-audit checks remain authoritative.

Account route snapshots merge additions and removals by shard across all affected owners. Each changed shard has one successful publication per batch; RCU retries preserve unrelated concurrent changes. Unchanged shards retain their Arc. Trade pruning rechecks the private epoch inside the RCU closure. GC removals also verify the expected owner.

The account mutation owner captures an immutable typed prune delta. The existing WAL writer builds JSON paths and serializes it; a prune is a coalescing barrier. Exact order, trade, tombstone, fee, compacted-economic and audit semantics are unchanged, including idempotent replay. The compacted economic summary is still cloned as typed data; it does not clone the full account ledger. Migrating that summary to leaf deltas is separate work if its measured capture time becomes material.

## Ownership and backpressure

Index rows belong to the existing instance lifecycle owner. The cold GC mailbox initiates requests through its sealed capability, but once bound the actual deletion executes on the private lifecycle actor through the existing command lane and control gate. The strategy-owned StrategyAccount remains separate. Identity changes replace a complete row; mutable access only changes status/economic fields. No new shared mutable map, mutex, worker or quote-path work is introduced. Live routing/persistence stays asynchronous on the existing lifecycle lane, not strategy quotation/admission. The legacy cold shared-account APIs still exist; they should migrate to owner messages rather than gain new callers.

GC request/completion queues retain their existing bounds and lossless retry behavior. Typed prune jobs use the existing 65,536-entry account persistence queue and generation ordering; overflow retains the existing fail-closed persistence blocker. No fsync/grace/wallet-watermark safeguards are reduced. Worker roles, affinity classes and topology are unchanged.

## Focused release measurements

macOS x86_64, `cargo test -p hexagent-account --release --lib benchmark_gc_index_routes_and_persistence_capture -- --ignored --nocapture`, 1,000 alternating samples per variant. Units below are microseconds. These are synchronous component boundaries (queue depth/overflow 0/0 by construction), not exchange RTT or end-to-end percentiles. Setup and fixture construction are excluded; returned-value destruction is included. Production queues and end-to-end stages require separate post-deployment observation.

| Boundary | P50 | P99 | P999 | Maximum |
|---|---:|---:|---:|---:|
| Scan 38,000 history rows for target token | 830.760 | 4584.651 | 5757.040 | 7676.689 |
| Indexed target candidates, budget 128 | 2.396 | 22.259 | 34.763 | 46.332 |
| 2,000 unchanged routes, per-key RCU then retain | 7701.453 | 10195.867 | 12419.967 | 12877.917 |
| Same snapshot, combined shard publication | 198.955 | 376.181 | 453.847 | 523.522 |
| Prune eight orders/eight trades, inline serde | 25.989 | 62.608 | 86.278 | 122.735 |
| Same prune, immutable typed capture | 4.985 | 22.672 | 27.768 | 35.628 |
| Plain history insertion | 0.199 | 0.566 | 0.716 | 2.019 |
| Insertion with derived indexes | 1.098 | 9.754 | 30.525 | 53.560 |

Indexes add allocation and memory on the existing private lifecycle/history owner; the insertion benchmark makes that tradeoff explicit. They remove global-history scans from GC while leaving the strategy quote path unchanged. Check private queue and private-to-strategy tails after deployment to ensure this cost is not merely shifted downstream. The route benchmark is repeated unchanged synchronization, which was especially wasteful before; it is not a changing-route throughput claim.

Before deployment, maker02 14:38:02–15:07:34 UTC: 182 GC owner attempts, maximum 23.99 ms (177 successful critical sections, maximum 23.96 ms); 2,059 lifecycle queue samples, maximum 869.9 µs; 168 private-WS-to-strategy-account samples, maximum 254.8 µs. These are separate one-minute histogram windows: maxima can be compared, but their P99/P999 must not be pooled into a whole-run percentile.

## Validation

Runtime: 54 tests. Account: 249 passed, seven opt-in benchmarks ignored. Coverage includes failed/truncated warmup with subsequent socket reuse for both transports; generation fencing; owner isolation; index replacement/removal/replay; cursor continuation through a protected prefix; immutable WAL replay and duplicate application; coalescing order; existing mailbox overflow and stale-certificate tests. See the deployment investigation artifacts for downstream exchange/engine validation and the production comparison.

Downstream exchange validation: all 627 tests passed with `--test-threads=1` (11 ignored). Parallel debug runs hit the pre-existing two-second test barrier in retired-trade replay tests under concurrent local compile/backtest load; the eight retired-trade tests also passed in isolation. No test timeout was relaxed.
