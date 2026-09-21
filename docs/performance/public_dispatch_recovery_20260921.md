# Public market dispatch under FIFO preemption

## Incident and mechanism

On maker02, the public readiness `bounded(1)` channel stopped at head
`0x7000b0`, tail `0x7000b4`, slot stamp `0x7000b0`. The Binance producer had
successfully reserved the slot but had not published its stamp. GDB placed it
immediately after the successful tail CAS. Binance ran SCHED_FIFO 50 on CPU 7;
a Polymarket producer ran FIFO 71 on the same CPU and spun in `start_send`.
The root router (FIFO 60, CPU 4) spun in `start_recv`. `sched_yield` could not
resume the lower-priority producer. A replacement feed and the supervisor
subsequently entered the same publication wait. Rebuilding the websocket did
not repair the stopped downstream mailbox. Perf attributed 93.23% of sampled
CPU to the router's `crossbeam_channel::flavors::array::Channel::start_recv`.

## Queue and scheduling contract

Both public lanes now use preallocated `TryQueue<MarketEvent>` slots. A try
operation reads one stamp and attempts one cursor CAS. An unpublished FIFO head,
a slot still being released, or a competing CAS returns immediately. Acquire /
release stamps transfer initialized values; only the successful cursor CAS
owner accesses the slot. Head/tail occupy separate cache lines. Capacity one,
non-power-of-two capacities and cursor wrap retain distinct empty/full stamps.

- Public feed/parser threads publish; the adapter feed owner or root router
  consumes. Existing capacities remain unchanged (adapter: 4,096 per lane).
- Ordered events retain reservation FIFO order. Capacity or contention returns
  the exact event/error to the existing fail-closed/reconnect path. Cold
  shutdown/control publication may retain/retry to a deadline.
- Replaceable snapshots attempt one push, at most one old-item eviction and one
  final push. An unfinished peer never causes a spin; a failed final push is
  counted and dropped. Successful eviction uses the same exclusive consumer
  CAS, so producer eviction and owner consumption cannot read the same value.
- Ordered events keep their eight-event burst budget before a latest snapshot.
  Private trade/order lanes and per-instance account ownership are unchanged.
- Producers no longer publish readiness notifications. Consumers use the select
  deadline: zero for a committed head, 10 microseconds for idle/uncommitted
  heads. The idle deadline parks the high-priority consumer, permitting lower
  priority producers to complete. OS wakeup delay may exceed 10 microseconds.
  Existing private/lifecycle select branches have priority over public data.
  Previously adapter channels disabled readiness publication but recv_timeout
  waited on a never-ready receiver, leaving idle arrivals until the outer 1 ms
  timeout. The bounded idle slices also remove that adapter wakeup gap.
- `ready_receiver()` is replaced by `poll_interval()`; consumers must re-evaluate
  it each select iteration. Recorder composition follows the same deadline.
  No timer `AtomicCell<Instant>` is added (its fallback can use a shared lock).
- The existing background supervisor exports scalar consumer heartbeat, queue
  depth/capacity and contention-drop snapshots every 10 seconds. The heartbeat
  cache line is separated from publisher admission state. It does not modify
  accounts or control process lifetime.

There are no new threads, affinity roles, queues that grow at runtime, or locks
in this mailbox. No process watchdog, forced-exit timer or service restart
policy is introduced. Existing public-event construction/destruction and root
`Arc<MarketEvent>` routing allocations remain a separate migration task.

## Validation

Deterministic tests pause a producer immediately after slot reservation and
verify later publishers/consumers return without waiting, preserve FIFO and
resume after publication. Tests also cover paused consumers, exact capacity,
non-power-of-two reuse, cursor wrap, concurrent producers/consumers with exact
once transfer, ownership destruction, receiver isolation, drain/disconnect,
idle arrival and bounded retry of the exact ordered event. Existing engine and
exchange suites exercise reconnect, lifecycle ordering, replay and ownership.

Focused before/after benchmark results and live observation are recorded with
the deployment report in hexbot. A publication microbenchmark is not a claim
about exchange ACK or private trade latency; live stages must be assessed
separately and histogram window percentiles must not be merged as raw samples.
