# Post-restart recovery and router investigation (2026-09-21)

The consumer investigation used hexbot's last normal restart, UTC
04:10:36.004–15:23:54.244, based on SDK `5e13cdc`. The strongest router sample
had a 945,744 us poll age and 478 pending public events while feed progress
remained current. This is evidence of delay before instance ingress, not proof
of a particular blocking stack.

## Changes and evidence boundaries

- Root router and strategy watchdog deadlines now belong to their respective
  owner threads. `crossbeam_channel::tick` uses `AtomicCell<Instant>`; its
  non-lock-free fallback uses process-wide hashed locks. A higher-priority FIFO
  router sharing a CPU with a lower-priority lock holder can invert priority.
  Removing those timers removes that mechanism; no sampled production stack
  established it as the cause of all historical stalls.
- Root progress publishes a scalar phase breadcrumb, formatted only by the
  background supervisor. Lifecycle retries happen before every router select;
  the public idle polling interval remains 10 us. Private lanes still precede
  public data, and watchdog work checks all lifecycle lanes before running.
- Cold wallet retries reconcile prior operations first, then replay confirmed
  seed allocations for the exact condition, token pair and instance amount.
  Partial/mismatched proof fails closed. Recovery/replay can proceed after the
  new-submit deadline; new chain work still cannot. Coherent startup seeds now
  pair confirmed split identities with balances so late messages cannot book
  the same split twice. The consumer must adopt the new seed field.
- HTTP connector failures and slot-wait deadline expiration are explicitly
  `NotSent` to the placement classifier. Ambiguous `SendRequest`, response loss,
  malformed success, and post-dispatch timeout remain unknown and reserved.
  The three historical 2–3 ms reused-socket failures are still ambiguous; this
  patch does **not** retroactively prove those orders absent.
- HTTP request deadlines now cover serialization wait as well as transport.
  A queued request that expires never touches the active request's trace and
  never retires its connection. Error chains retain underlying transport causes.
  There is no automatic POST replay, extra connection pool, or shorter live
  order timeout.

## Ownership and backpressure review

1. Each `OwnerTimer` is a plain value owned and mutated by its router/strategy
   thread. Startup split identities transfer in the existing immutable seed;
   the strategy alone changes its `applied_splits`.
2. Existing lifecycle FIFO/outboxes, wallet queue (64), and inventory-ready
   channels retain capacity, priority, recovery and overflow behavior. This
   patch introduces no queue or worker. Cold wallet reads consume published
   snapshots or the existing cold mirror, never borrow a live strategy object.
3. Router phase writes are scalar atomics. No new hot-path allocation, shared
   lock, I/O, log formatting or histogram aggregation is added. Existing
   `Arc<MarketEvent>` allocation and channel wakeup locks remain migration work;
   this patch does not extend their scope.
4. Subscription routing and exact per-instance lifecycle identities are
   unchanged. Replay proof must match owner, token pair and funded quantity.
5. Worker placement/scheduling is unchanged; no additional CPU class is needed.
6. The benchmark below measures timer/select/pop only. It cannot establish an
   improvement to production end-to-end tail latency or rule out a downstream
   delay. Deployment observation must include public pending/poll age/phase,
   instance ingress, private apply and HTTP audit latency together.

## Validation

`cargo test -p hexagent-account -p hexagent-engine -p hexagent-exchange -p
hexagent-runtime --lib`: 324 + 134 + 903 + 70 tests passed; 56 tests ignored by
existing/manual test annotations. Regression cases include FIFO/overflow,
replay/duplication/isolation, recovered seed scope, coherent startup identity,
server-observed POST response loss versus connect refusal, and serialization
queue expiry without sending.

Release benchmark, macOS local machine, unpinned; 200,000 observations per
variant. Boundary: after a public item is queued, timer/private-channel select
and bounded queue pop; two empty private lanes, same queue in both variants.
No network, strategy callback or exchange acknowledgement is included.

| Timer implementation | N | Median ns | P99 ns | P999 ns | Maximum ns | Queue high water | Overflow |
|---|---:|---:|---:|---:|---:|---:|---:|
| crossbeam tick + lifecycle retry tick | 200,000 | 519 | 897 | 1,003 | 49,578 | 1 | 0 |
| owner-local deadline | 200,000 | 324 | 334 | 339 | 27,382 | 1 | 0 |

Run: `cargo test -p hexagent-runtime --release owner_timer -- --ignored
--nocapture`. Raw output and test summaries are under
`docs/evidence/post_restart_recovery_20260921/`.
