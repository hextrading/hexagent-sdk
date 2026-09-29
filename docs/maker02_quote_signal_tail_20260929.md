# maker02 quote-signal scheduling tails

## Evidence and attribution

The 2026-09-29 11:41–12:26 UTC production window contained quote-to-executor
histogram maxima of 989.340 ms. Exact slow-order timestamps give 938.922 ms
(11:41:36.221) and 988.626 ms (11:52:32.740). Those signals were rejected as
stale before HTTP POST. They are therefore absent from the otherwise healthy
7,130 sent-order trigger-to-dispatch samples (P50/P99/P999/max
0.271006/0.599960/0.772907/0.854178 ms).

The corresponding one-minute `/proc` samples show the signal arbiter runnable
but unscheduled for 939.175/992.451 ms; the execution dispatcher consumed
1,128.081/1,149.452 ms CPU. Both share CPU 4 at FIFO 50. The higher-priority
market router retained its normal roughly 8.7 seconds CPU per minute. Cold
recovery queries entered just before both stalls. This identifies dispatcher
interference, rather than slow strategy computation, as the affected lane.

Crossbeam 0.5.15 bounded `start_recv` spins when a producer has advanced the tail
but has not published its slot. `try_recv` is therefore not a bounded-time
operation in that state. A same-CPU FIFO consumer prevents a SCHED_OTHER
producer from completing. The host's RT period/runtime is 1,000,000/950,000 us.
Additionally, selecting receivers and publishing replaceable health snapshots
uses Crossbeam wake mutexes. Sampling found execution stacks in
`SyncWaker::unregister` below `run_select`, and `SyncWaker::notify` below
`refresh_health`. These are concrete dependencies removed here, **not an exact
historical stack attribution for the 11:52 incident**. Online profiling disturbed
scheduling/admission during approximately 13:17–13:19 UTC; it was stopped and
normal quoting recovered. That interval is excluded from latency comparisons.

## Ownership, queues and scheduling

* Recovery/probe HTTP ingress remains capacity 64. Existing cold producers use
  the preallocated polling FIFO; the execution dispatcher is its sole consumer.
  An unpublished head returns Empty without overtaking it. Full/contention or
  disconnection is explicitly not-sent; durable cancel/recovery owners retain
  retry responsibility. An admitted POST with a lost reply remains ambiguous.
* The dispatcher polls shutdown, lifecycle, strategy signals, then cold HTTP in
  the former priority order. It never registers select waiters. An idle or
  unpublished cold head sleeps for the existing 10 us poll duration. This sleep
  is in the dispatcher idle path, never inside a strategy quote callback.
  Owner-local timers retain the 1 ms idle health/cancel maintenance cadence;
  completed work also requests maintenance on the next turn.
* Each replaceable health/admission lane has one producer, one consumer, three
  startup-allocated physical slots and capacity for one unread latest snapshot.
  An AcqRel slot exchange transfers ownership without locks, waiting, retries,
  notifications or allocation. Only Copy snapshots use this path. Cumulative
  connection failure counters, sequence/epoch checks and the one-second
  fail-closed admission lease retain their existing meaning.
* Diagnostic output remains capacity 1,024, advisory drop-on-full with its
  existing overflow counter. The existing background worker drains batches
  between 10 ms idle sleeps. No formatting or IO moves onto a critical thread.
* The dispatcher owns both new timers; snapshot endpoint indices belong only
  to their owning thread. Cold HTTP producers update advisory occupancy/reject
  counters; those counters never grant order admission. The dispatcher forwards
  periodic compact queue diagnostics to the background logger. The new
  `execution.probe_ingress_wait` measures envelope creation through dispatcher
  dequeue, before any HTTP execution.

No worker, affinity, priority, order reservation or private lifecycle lane is
added or changed. Account/strategy mutable state remains owner-local. Legacy
signal/control channels still exist; replacing them requires retaining their
shutdown barriers and per-owner overflow rollback and is outside this bounded
migration. In particular, the existing signal arbiter may wait on full root
capacity; this patch does not claim every legacy channel is wait-free.

## Focused verification

Release runtime suite: 88 passed, 6 intentionally ignored manual benchmarks.
Snapshot tests cover paused publication, repeated replacement, concurrent
non-torn/non-replayed values, independent instances and final drain before
producer disconnect. Polling FIFO tests cover unpublished heads, saturation,
exact retained messages, disconnects, ordering and concurrent producers.

Controlled before/after benchmark on the development x86_64 macOS host:
100,000 single try-receive calls per variant, including 200 cold producer
reservations deliberately held for 10 ms. The measured boundary is entry to
`try_recv` through its return, allowing the next independent quote check; this
excludes HTTP, dispatcher idle sleep and an actual strategy callback.

| Variant | N | P50 ns | P99 ns | P999 ns | Max ns | Queue high / overflow |
|---|---:|---:|---:|---:|---:|---:|
| Crossbeam bounded | 100,000 | 100 | 132 | 10,800,392 | 39,012,246 | 1 / 0 |
| Polling FIFO | 100,000 | 78 | 99 | 517 | 17,413 | 1 / 0 |

The same source-only harness was also compiled offline on maker02's aarch64
host, isolated to housekeeping CPUs 0–1 at SCHED_OTHER / nice 19 (no profiling
or attachment to the trading process). All 16 FIFO/snapshot tests passed.
The same 100,000-call / 200-preemption benchmark produced:

| Variant | N | P50 ns | P99 ns | P999 ns | Max ns | Queue high / overflow |
|---|---:|---:|---:|---:|---:|---:|
| Crossbeam bounded (aarch64) | 100,000 | 52 | 55 | 10,052,060 | 10,074,333 | 1 / 0 |
| Polling FIFO (aarch64) | 100,000 | 40 | 42 | 74 | 14,669 | 1 / 0 |

Both variants preserve the exact pending message for subsequent delivery.
The benchmark establishes the preemption dependency and its removal; it does
not establish a production maximum or substitute for the rollout comparison.
Production comparison must include all quote-to-executor observations, stale
rejections, actual sent-order latency, downstream queue depth/overflow and
private-trade application, using a fixed post-restart window.
