# Polling execution-result lane for the live router

The 03:26:34–05:26:34 UTC maker02 observation completed without a large router
pause. At 05:33:04.730, the same process subsequently reported phase 3 (select),
poll age 948,268 us and 87 pending market events. Its retained maximum became
948,667 us. The incident-minute receive-to-router maximum was 948.70 ms, while
strategy queue/callback maxima were 74.1/70.2 us. These are separate windows.

The execution-result input still used `crossbeam-channel 0.5.15`'s bounded
array. `start_recv` waits when the tail has advanced but the head slot has not
been published, including from `try_recv` and selection. Live execution and
router share CPU 4 at FIFO 50 and FIFO 60. Thus a preempted publication can make
the higher-priority router wait for its own lower-priority producer. The host's
RT bandwidth settings are 950,000/1,000,000 us. This is a concrete remaining
priority-inversion mechanism; duration and phase are consistent with this
incident, but there is no contemporaneous stack proving that attribution.

## Ownership and recovery

Live execution-result root delivery now uses a capacity-10,000 polling MPSC
channel over the existing `TryQueue`. Each try operation returns after a fixed
number of atomic attempts and never waits for another thread to complete its
slot. The one router consumer checks the execution lane before public data.
When a private reservation is pending but uncommitted, it sleeps for 10 us and
retries on its next turn: public events cannot overtake that reservation.

Completion outbox / execution producers retain the exact unsent value on full
or contention, sleeping between attempts as the old blocking-send sites already
waited for downstream capacity. They fail only on receiver closure. Capacity,
per-producer order, owner stamps and existing per-instance lossless outboxes are
preserved. Quote callbacks do not call the blocking send API. No new worker,
lock, global map, notification channel or live account access is added.

The router samples execution depth before pop and publishes only a scalar
lifetime high-water mark; the existing supervisor formats/exports it. This is
an observed high water, not an exact concurrent peak. Existing admission,
quarantine and lifecycle-overflow behavior are retained. Shutdown drains the
execution tail, including a still-pending reservation, before closing owners.

The full-router test also exposed an existing non-Polymarket/paper issue:
without a SharedState retaining direct-private senders, their priority receivers
disconnected immediately and workers exited. The router now retains these
startup-owned senders through its lifetime and closes them before worker joins.

Compatibility private/recovery channels, paper execution input, physical
connection outboxes and strategy input channels remain crossbeam. They should
be migrated separately with their own priority, wakeup and backpressure tests;
this change does not claim to remove every crossbeam wait or every router tail.

## Validation and measurement limits

Queue tests cover a producer deterministically paused after reservation, exact
capacity, full-lane retry, receiver closure, drain before disconnect, isolation,
and three concurrent producers delivering 6,000 records exactly once in
per-producer order. The actual router/worker test interleaves two numeric owners
and replayed updates, verifies private-before-market delivery, checks high water,
and completes the normal shutdown handshake. Existing execution and startup
replay tests continue to run.

Local release microbenchmark: each of 100,000 prebuilt u64 records is pushed and
popped on one thread using capacity 10,000; samples are preallocated, queues
are initialized before timing, depth high water 1, dropped 0. Tests run serially.

| Transport | N | P50 ns | P99 ns | P999 ns | Max ns |
| --- | ---: | ---: | ---: | ---: | ---: |
| crossbeam 0.5.15 | 100,000 | 54 | 59 | 66 | 13,873 |
| polling lane | 100,000 | 43 | 47 | 54 | 13,724 |

This measures queue API cost on the local macOS machine, excluding producer
preemption, wakeup, owner callbacks and exchange I/O. It cannot establish Linux
end-to-end latency improvement. Live verification must also inspect
`polymarket.update.producer_to_root_router`, root-to-instance, market ingress,
execution high water, overflow, private terminals and inventory convergence.

The earlier CPU-progress-only watcher cannot detect a busy spin. The follow-up
watcher also captures >=100 ms windows above 80% router CPU, alongside bounded
99 Hz rolling perf data. A busy interval is a trigger for investigation, not
proof of the offending function.
