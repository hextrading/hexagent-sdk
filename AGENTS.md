# Hexagent SDK Permanent Low-Latency Principles

These instructions apply to the entire SDK repository and to every future
design, implementation, review, refactor, and performance investigation.

## Mandatory architecture principles

- Give every strategy instance a dedicated thread, private virtual account, and
  private mutable runtime state. Initialize and bind them at startup; after that,
  only the owning strategy thread may mutate them.
- Strategy instances communicate only through explicit messages. Do not expose
  shared mutable account or strategy state, and do not use a shared account as a
  runtime authority or final cross-instance admission gate.
- Keep market-data-to-quote and quote-to-dispatch paths free of global mutable
  state, contended locks, dynamic allocation, blocking operations, synchronous
  logging/persistence/statistics, and redundant conversion or cloning.
- Route market data only to subscribed strategy workers. Route private trades,
  order lifecycle events, and execution results asynchronously to exactly one
  owning strategy instance.
- Move account aggregation, reporting, metrics export, logs, reconciliation,
  audit, and persistence to dedicated background workers outside critical lanes.
- Prefer startup-time/preallocated objects, bounded lock-free or single-writer
  queues, cache-friendly fixed-capacity data, explicit backpressure behavior,
  CPU affinity, and batched asynchronous writes.
- Optimize and review against end-to-end latency and tail latency (`P99` and
  `P999`), not only average or isolated function latency. Measure market receipt
  through strategy decision and execution dispatch, then acknowledgement and
  private-trade application as separate attributable stages.

## Required implementation discipline

- Every mutable object has one documented owner thread. Cross-thread consumers
  receive owned messages or immutable published snapshots, never a borrowed live
  mutable object.
- Every queue documents its producers, consumer, priority, ordering, capacity,
  overflow policy, and recovery behavior. Private trade/order lifecycle lanes
  are lossless and higher priority; replaceable market snapshots may use bounded
  latest-value lanes.
- Do not add `Arc<Mutex<_>>`, `Arc<RwLock<_>>`, process-global mutable maps,
  unbounded queues, or synchronous cross-thread requests to a critical lane
  without explaining why ownership transfer or a single-writer worker is
  insufficient and supplying latency/backpressure evidence.
- Steady-state quote and dispatch processing must not grow the heap. Reuse
  buffers, envelopes, signal slots, and order builders; prefer fixed-capacity or
  stack-backed values. Profile and justify any unavoidable allocation.
- Never do file/network/database I/O, chain RPC, account snapshotting, log
  formatting, histogram aggregation/export, or retry sleeping in the quote path.
  Publish compact records and let a background worker batch the work.
- Preserve strategy ownership identity end to end. Ambiguous order or private
  event ownership fails closed outside the quote path.
- Every long-lived thread has an explicit role, affinity class, scheduling
  policy, and topology-validation entry. CPU placement is correctness in strict
  live low-latency mode.
- Performance changes require before/after evidence with event count, median,
  `P99`, `P999`, maximum, queue depth/overflow, and exact measurement boundaries.
- Tests cover ordering, duplicate/idempotent delivery, overflow, reconnect/replay,
  and instance isolation. Optimizations must retain fail-closed risk semantics.

## Change review gate

Before completing a change, verify:

1. Which thread owns each new mutable field?
2. Which messages cross threads, and what are their capacity, priority, ordering,
   overflow, and recovery semantics?
3. Does a critical steady-state path lock, allocate, block, format, log, persist,
   aggregate statistics, or consult a shared account/global map?
4. Is market data routed only to interested instances and is every private event
   delivered asynchronously to exactly one owner?
5. Are new workers affinity-managed and topology-validated, and are background
   writes asynchronous and batched?
6. What happened to end-to-end `P99`/`P999`, and did downstream queues merely
   absorb the latency?

If an existing subsystem violates these principles, do not deepen the violation.
Keep new work ownership-local and message-driven, and document an incremental
migration when safe removal is outside the current change.
