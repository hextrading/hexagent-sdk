Cold account commits previously replaced the durable tree with an aggregate
snapshot. The aggregate lifecycle mirror can lag an already-persisted cancel,
so an unrelated checkpoint or maintenance update could reopen the persisted
order and restore its released reservation. A late HTTP cancel acknowledgement
also cleared stronger private-order audit metadata.

Cold commits now publish immutable before/after transaction images. The existing
WAL writer applies only their changed fields to its current state. Typed field
comparison skips serialization of unchanged historical maps; an exhaustive
struct pattern requires every future state field to select a persistence policy.
Audit sets use per-member edits. Cold-only counters retain their previous
piggyback persistence behavior. Checkpoints and submitted maintenance records
use narrow entry updates instead of capturing historical account state.

HTTP cancellation without matched quantity or trade IDs preserves an existing
terminal audit, its residual reservation, and its outstanding trade-recovery
obligation. Duplicate weak replies do not requeue the audit or override a
stronger terminal status. Capture/serialization failure fails closed instead of
falling back to an unversioned full snapshot; subsequent cold publication cannot
reopen admission after that failure.

**Ownership and transport.** The new capture object belongs to its existing cold
transaction guard. Before/after images become immutable before entering the
existing bounded persistence lane (65,536 jobs); only the existing WAL worker
materializes and applies them. No new worker, queue, shared mutable map, lock, or
quote-path I/O was introduced. Atomic generation ordering, contiguous replay,
overflow failure, and transactional write rollback remain in place. Cold and
arbitrary field changes are coalescing barriers so earlier owned-row updates
cannot disappear across a partial update. Private routing and CPU placement are
unchanged. The on-disk snapshot/WAL schema remains version 1.

**Validation.** Both incident regressions were reproduced against production SDK
commit `5c00a20ded493e4de85cd18f402c49f28182095c` before the fix. Coverage includes a
deliberately parked cold mirror, independently flushed raw WAL prefixes followed
by restart, reservation conservation and sibling-instance isolation, both
HTTP/private-event arrival orders, repeated weak replies, zero and partial
matches on both sides, late trade application, audit-set replay idempotence, and
capture failure followed by cold/fee publication. Existing suites also cover
bounded overflow, reconnect/replay, writer failure rollback, and settlement/GC.

The focused local debug-build benchmark uses 1,000 historical order rows
(409,721 serialized baseline bytes), 300 events per mode. Boundaries include
capture, materialization, and in-memory WAL application; they exclude disk I/O,
queue waits, networking, and strategy execution. It does not measure production
end-to-end latency. The old implementation is retained only as a comparison
inside the ignored benchmark.

| Mode | Median | P99 | P999 | Maximum |
|---|---:|---:|---:|---:|
| Previous full-snapshot path | 17.604 ms | 23.584 ms | 23.847 ms | 23.847 ms |
| Cold transaction delta | 1.227 ms | 1.450 ms | 1.841 ms | 1.841 ms |
| Narrow checkpoint entry | 8.186 us | 43.841 us | 59.494 us | 59.494 us |

All three direct microbenchmarks have queue depth 0 and overflow 0 because no
queue is used within that boundary. Queue overflow is separately tested on the
real bounded worker lane. No production P99/P999 improvement is inferred from
these numbers.

Complex cold operations still clone transaction images before publishing them;
this is outside quote processing. Incremental migration should replace remaining
complex cold-image captures with explicit domain deltas and continue moving
cross-owner economic adjustments onto owner messages. This change addresses
unrelated stale-mirror replacement and weak-cancel audit downgrade; it does not
claim to redesign every existing shared-account economic mutation.
