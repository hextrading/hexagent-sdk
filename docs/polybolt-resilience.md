# PolyBolt connection resilience and cancel recovery

## Incident and evidence boundary

On maker02, 2026-11-03 10:26:55–10:27:06 UTC source seconds were absent
from both native BTC Chainlink series. The adapter reported two 15-second
freshness stalls. Delivery resumed with source 10:27:07 at receive 10:27:27.8.
The market recorder admitted/consumed counts matched, with no overflow or write
error. This localizes the failure before publication, but without a second
simultaneous wire capture does not distinguish provider, edge or connection.
Historical snapshots cannot repair the original timely-observation record.

Six trading request timeouts in the preceding observation window reached the
2-second first-byte deadline on both CLOB edge IPs. Connection repair also
encountered failing GET prewarms. Socket/TLS timing and local queue evidence do
not support increasing deadlines or replaying unknown orders. Those policies
are unchanged. Slow ordinary cancel lanes additionally waited minutes for a
new business request because healthy lanes always outranked probation lanes.

## Changes and ownership

- Two independent authenticated PolyBolt sockets use the existing public-feed
  reactor. Each coroutine owns its sequence, subscription, freshness and
  reconnect state; no new OS thread or affinity class is introduced.
- Both publish to the existing bounded 10,000-event replaceable public lane.
  Its existing drop-oldest/overflow accounting is retained. The public adapter
  consumer alone owns preallocated per-symbol merge watermarks. Earliest dequeued
  live observations advance time; duplicates/older observations are discarded.
  Equal-current-source conflicting prices stop the adapter. Receive/source
  clocks are preserved. Each poll examines at most 32 records, with no new heap
  growth, locks, I/O or logging in the merge path. Private lanes are untouched.
- Snapshots consume sequence but never enter the market lane, refresh liveness
  or seed live strike/settlement caches. When record-mode market audit is enabled,
  their individual points are archived as `polybolt_history/snapshot` in its
  existing bounded background-writer lane. Audit overflow remains explicit.
- Connection failure diagnostics identify the independent lane, connection,
  subscription acknowledgement, last source timestamp and freshness age.
- The account-local execution router promotes one oldest ordinary failed cancel
  candidate to round-robin rank after its existing backoff plus 30 seconds.
  At least one verified ordinary healthy cancel lane must remain. The selected
  candidate stays fixed while busy, so busy promotion cannot successively elevate
  all failed siblings. Existing physical fallback semantics remain in force.
  This routes already-required cancels; it does not create test orders/cancels.
- Expired/quarantined/retired lanes cannot be promoted, and emergency capacity
  cannot count as ordinary healthy coverage. Timers and GET prewarms still cannot
  prove business health; fresh real responses and existing account recovery
  checks remain required. Diagnostics add `ready_cancel` and `failed_cancel_mask`.

Two sockets mitigate a single-connection stall; they cannot guarantee delivery
during a common upstream outage. A late point older than the merged watermark is
not backfilled into a live recording. Independent wire/snapshot captures must
be kept separately when diagnosing archival completeness.

## Validation

Focused tests cover a real twelve-second WebSocket stall, independent sibling
progress, reconnect/sequence gaps, snapshot exclusion, duplicate/replay ordering,
source clocks, symbols/instance isolation, conflicting values, bounded duplicate
drain, overflow, cancellation probation and unchanged fail-closed recovery.
Run:

```
cargo test -p hexagent-exchange polybolt --lib
cargo test -p hexagent-engine execution_admission --lib
cargo test -p hexagent-engine owner_handoff_benchmark --lib
cargo test --release -p hexagent-exchange redundant_merge_benchmark -- --ignored --nocapture
cargo test --release -p hexagent-engine cancel_probation_benchmark -- --ignored --nocapture
```

The benchmarks include baseline/current arms and report count, median, P99,
P999, maximum, queue bounds and exact boundaries. They do not measure network
or strategy end-to-end latency. Native maker02 measurements and post-deployment
queue/latency/price checks are recorded in the Hexbot rollout evidence.

Protocol reference: https://docs.polymarket.com/api-reference/live-data/overview
(checked 2026-11-03): per-connection/channel sequences include snapshots,
snapshots contain two minutes of history, and pinned provider/window semantics
must be validated independently on every connection.
