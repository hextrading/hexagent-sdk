# Root routing under ordered public-trade bursts

The maker02 2026-09-28 05:17–05:47 UTC window reached 6.34 ms from
market receipt to strategy callback. Root Binance queue age reached 6.28 ms,
Coinbase 4.81 ms; downstream strategy queue maximum was 171.1 us. Root backlog
rose to 961 records while its maximum poll gap remained 209 us at the incident.
The recorded Binance stream at 05:33:25 contains 2,221 trades in 9.679 ms,
including 478 arrivals within one millisecond. This is sustained drain cost
under a burst, not evidence of a single six-millisecond scheduling pause.

## Ownership and transport

- The existing root router remains the sole producer of each strategy market
  lane, and each existing strategy thread remains its sole consumer. No new
  worker, CPU role, shared account, mutable registry, or trading parameter.
- Each market lane keeps its 10,000-record capacity. The preallocated polling
  FIFO replaces crossbeam's market sender/consumer wake registration. Ordered
  trades, bars and lifecycle records retain FIFO, identity and fail-closed
  overflow. Latest markers retain their existing key/generation/epoch barriers;
  a trade or lifecycle boundary cannot be coalesced away.
- Ready root control messages are polled directly, then recovery-private,
  execution lifecycle and market events, with the existing lossless outboxes.
  An unfinished private publication still yields without allowing market work
  to overtake it. Root idle waits are nominally 10 us, after an empty pop.
- Strategy input arbitration preserves admission, private-control, direct
  private, compatibility-private, execution-lifecycle, history, then market
  priority. The ready path never registers waiters. Only the idle path uses
  timed private/control select; new market arrivals are observed within the
  next nominal 10 us poll plus scheduler delay. Private events retain their
  immediate wake path. The existing startup-intake pause and fail-closed
  shutdown behavior remain in effect.
- New measurements enqueue compact observations to the existing background
  latency worker: `market.router.dispatch` (Arc construction through subscribed
  fan-out) and `strategy.market.pending_depth` (records pending before pop;
  values are counts, despite the generic latency export's ns display).
  Chainlink RTDS and Data Streams now have explicit adapter/root stages.

## Remaining migration boundaries

Root Arc construction and existing event payload allocation are unchanged.
Other crossbeam control/private/recorder lanes remain; this change does not
claim that the entire engine is allocation-free or lock-free. Those lanes
require separate ownership/priority evidence before migration. Polling trades
small additional idle wake/CPU cost and nominal market wake delay for avoiding
producer-side market wake locks. Validate total callback latency and downstream
queue depth, not just the now-shorter root queue.

## Validation

`root_router_ordered_burst_benchmark` invokes the actual root and three strategy
owners, sends 20 waves of 1,000 ordered trades, and validates every per-owner
sequence (no loss/reorder/duplication). Its measured boundary is producer stamp
immediately before root publication through strategy callback; clock warmup and
payload construction occur first. It reports N/P50/P99/P999/max, root queue high
water, pending and contention loss. `HEXAGENT_ROUTER_BENCH_TRACE` optionally
supplies newline-delimited relative arrival nanoseconds, replayed by the test
producer only. This never connects to an exchange or emits orders.

Functional coverage includes existing root recovery/lifecycle replay, strict
priority, per-instance isolation, latest-marker ordering/overflow, startup
lifecycle intake and shutdown tests; polling transport tests cover capacity,
preempted publication, concurrent producers, disconnect and cold deadlines.

Before/after benchmark and live-window results are recorded by the consuming
hexbot deployment report. A CPU sample profile dominated by idle select is not
interpreted as a duration breakdown of the earlier burst.

### Pre-deployment results (2026-09-28)

Engine: 148 passed / 8 ignored, both local debug and maker02 Linux release;
Exchange: 935 passed / 45 ignored; Runtime: 81 passed / 5 ignored.

Five alternating recorded-arrival replays, each version totalling 222,100 input
trades / 666,300 owner callbacks, on maker02 CPUs 0–1, SCHED_OTHER nice 19,
with pin/FIFO overrides disabled via `HEXBOT_NO_PIN=1 HEXBOT_NO_FIFO=1`:

| Producer enqueue to callback | Before | After |
| --- | ---: | ---: |
| Median run P50 | 199.177 us | 130.142 us |
| Median run P99 | 2703.076 us | 875.164 us |
| Median run P999 | 4013.008 us | 2398.946 us |
| Worst observed maximum | 10558.623 us | 2848.904 us |
| Maximum sampled root backlog | 1166 | 681 |
| Ordered loss / duplication / contention drops | 0 | 0 |
| Remaining root backlog | 0 | 0 |

These are medians of run quantiles, not pooled event quantiles. The constrained
housekeeping CPU test is distinct from production's dedicated strategy CPUs.
An instantaneous synthetic 1,000-record burst did not improve P99 on those two
CPUs (2328.693 → 2540.767 us, one run each), so no universal latency bound is
claimed; live end-to-end and downstream-queue observations remain required.

The initial cold debug-only cancel-outbox timing check included first telemetry
slab construction within its existing 10 ms assertion; full engine suites and
the baseline release isolated check pass. The assertion and cancellation logic
are unchanged. Raw traces, exact source archives and all test output are retained
under hexbot `docs/evidence/maker02_router_tail_20260928T0610Z`.
