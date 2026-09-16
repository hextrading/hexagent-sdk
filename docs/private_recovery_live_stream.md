# Private stream recovery during an unavailable order audit

After a private WebSocket reconnect, a successful trade-gap replay could be
followed by a single order GET returning `null`. Previously, this failed the
whole order-audit result and disconnected the socket before its read loop
started. Successful sibling audit updates were also discarded even though the
cold account could already have committed their terminal state.

After the gap completes, the socket now reads private frames and sends
heartbeats while one independent order-audit job runs. Unknown results retain
the recovery gate and reservations. Successful sibling updates are delivered
on every attempt. Retry delays are 250 ms, 500 ms, 1 s, 2 s, 4 s, 8 s, 16 s,
then at most 30 s. REST uncertainty alone does not reconnect the socket. A
previously buffered startup update is released once its strategy consumer is
ready, including when the audit still reports an unrelated unknown order.

## Ownership and recovery boundary

- The socket task owns `OpenOrderRecovery`, its retry schedule, generation,
  optional job handle and fence receiver. There is at most one audit/forward
  or delivery job in flight per account. Jobs use the existing blocking
  runtime; no permanent worker, topology role or quote-path operation is added.
- The existing private owner receives Live messages on its bounded FIFO of
  1,024 commands. Replay has its existing separate 1,024-command lane; live
  traffic retains priority. Full or disconnected live lanes trigger recovery
  and replay; they do not overwrite private lifecycle events.
- During the audit, Live carries the same recovery generation as gap and audit
  updates. Each update is registered before lossless delivery to its exact
  owning strategy. Existing startup storage is bounded at 4,096 updates;
  pending delivery and its control lane each have capacity 16,384. Full,
  missing-owner or disconnected delivery remains fail-closed. A cold job owns
  unsent audit/startup updates until delivery or process shutdown; a retry does
  not discard them or register an already-enrolled startup update twice.
- A successful complete audit inserts `RecoveryFence` into the same Live FIFO.
  The socket switches subsequent Live frames to ordinary delivery in the same
  synchronous step. The private owner closes enrollment only after preceding
  tagged frames have registered their updates. The strategy acknowledges each
  update only after applying it. Fence completion alone does not clear recovery.
- A real disconnect drains the retained job, fence and strategy acknowledgments
  before starting another generation. An old result cannot certify the new
  socket. A task panic or a disconnected fence owner stops the feed with the
  gate closed; blocked update delivery retains the job and its batch. Cold-ledger
  failures independently assert recovery as before.
- The gap/audit/delivery proof captures a recovery certificate. Every new
  recovery assertion advances the packed atomic epoch, including while already
  paused. Clearing recovery is a compare-and-swap against that certificate;
  an intervening cold-owner failure cannot be overwritten by a late success.
  Inventory uncertainty and existing order/private risk gates remain in force.
- Delayed Accepted updates preserve a fully `Matched` order. Only a failed
  constituent private trade can reopen it through the existing Failed path.
  Trade deduplication, cancelled-order handling and reservation ownership are
  unchanged.

## Scope and existing transport debt

Trade-gap recovery still precedes the concurrent socket/audit phase. A failed
gap retains its cursor and follows the existing reconnect behavior. This change
therefore does not promise uninterrupted reading during a gap REST failure.
It also does not prove that an order returning `null` will become known sooner:
there may be no private event if the original submission was never accepted.
`null` is not terminal proof, and no POST retry or synthetic test order is added.

The existing audit attempts to borrow Reconcile permits. When live connection
owners retain those permits, the audit falls back to the process-wide Query
transport; the parallel two-slot absence check may then be unavailable. This
patch bounds repeated load through one audit job and backoff, but does not
repair that existing capacity-isolation limitation. The incremental migration
is an account-owned cold HTTP execution lane allocated at startup, with bounded
request/result messages, explicit permit ownership and account/strategy identity,
followed by transport-specific latency and overflow evidence. Do not add an
extra shared mutable gate or borrow a live owner's permit concurrently.

## Verification and measurement boundary

The full exchange suite passed **767 tests, 21 ignored**. A subsequent focused
run after the final delivery-panic guard and its regression passed **72 user
feed tests**. The focused run overlaps the full suite; these counts must not
be added. The final account suite passed **275 tests, 10 ignored**, including both
delayed-Accepted/Failed sequence regressions (the overlapping order-manager
subset passed 22 tests). Functional coverage includes
actual production recovery polling with a pending/null fake audit, real private
owner delivery, FIFO fences, ACK ownership, duplicate ACKs, startup buffering,
reconnect drain, task panic, account-local certificates and queue saturation.

A local unoptimized x86_64 macOS microbenchmark measured the production
`select_ws_or_recovery` boundary from an already-classified ready frame to a
successful `dispatch_live` enqueue. Each scenario has 100,000 measured events
following 4,096 warmup events. Nearest-rank nanosecond quantiles:

| Scenario | N | P50 ns | P99 ns | P999 ns | Maximum ns |
|---|---:|---:|---:|---:|---:|
| Normal audit | 100,000 | 606 | 804 | 998 | 15,288 |
| Persistent null audit | 100,000 | 513 | 620 | 786 | 31,642 |

Both measured runs reached queue depth 64/1,024 with zero overflow and verified
FIFO order. A separate boundary test accepted exactly 1,024 entries, rejected
entry 1,025 explicitly, and preserved all accepted entries. Initial lazy clock
initialization took 200,105,473 ns in the first warmup dispatch and is reported
separately, not hidden inside a steady-state quantile. Fixture allocation,
backend completion processing, HTTP/WS transport, JSON parsing, strategy
application and persistence are outside the timed samples. These are neither
production end-to-end measurements nor an old/new performance comparison.
The final benchmark includes the startup-flush, panic-handling and
late-Accepted fixes; its evidence records the exact source hashes.

Raw evidence is retained in the hexbot repository under
`docs/evidence/maker02_admission_rollout_20260916/`:
`private_recovery_exchange_tests.log`, `private_recovery_user_feed_tests.log`,
`private_recovery_test_scope.json`, and `private_recovery_benchmark.{json,log}`.
The benchmark is reproducible with
`cargo test -p hexagent-exchange --lib --no-default-features recovery_bench -- --nocapture --test-threads=1`.
