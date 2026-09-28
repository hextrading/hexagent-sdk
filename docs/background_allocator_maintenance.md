# Optional background allocator maintenance

Publication on maker02 still showed a 34.30 ms maximum in a copy-on-write
snapshot stage after removing reservation-table rehashes. CPU time was 34.29 ms
and measured off-CPU time at most 1.1 us in that minute. Earlier Linux samples
identified mimalloc global arena purging below these allocation sites; the new
spike has not itself been sampled. Merely changing MADV_DONTNEED to MADV_FREE
had not eliminated allocation-triggered purge work.

Applications can now register an optional allocator maintenance function before
starting the existing latency worker. The SDK stays allocator-neutral. The
worker snapshots this immutable startup function once, invokes it at most once
per second after draining observations, and measures the entire invocation as
`runtime.allocator.background_maintenance`. A delayed call resets its deadline
without executing a catch-up burst. Applications must use thread-safe allocator
APIs and monitor duration and observation queue overflow when enabling it.

The `AllocatorMaintenance` deadline has exactly one writer: `latency-dump`.
There are no new messages, queues, account reads, strategy state, or hot-path
operations. The existing worker retains its background CPU class, SCHED_OTHER
policy, topology validation and unified shutdown. Observation FIFOs retain
65,536-entry bounded capacity and their existing metrics-only drop policy;
private/order lifecycle channels and their lossless semantics are unchanged.
A blocking allocator invocation can delay this worker's next drain/shutdown,
so rollout must check both queue high-water/drop and measured duration, rather
than presenting a shifted delay as an end-to-end improvement.

Release runtime suite: 80 passed, 5 ignored. Tests cover optional/absent hooks,
rate limiting and delayed ticks without catch-up, actual execution on the
latency-dump thread, independent observation draining, bounded FIFO order,
overflow/recovery, producer/consumer preemption, isolation, and shutdown wakeup.
The maker02 application rollout supplies allocator-specific Linux measurements,
reclaimability and an actual 30-minute production comparison; enabling this
optional API alone makes no allocator or end-to-end latency claim.
