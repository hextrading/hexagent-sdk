# Private recovery root lane: complete the live polling migration

PR #276 moved execution completions to a polling queue but left private/reconnect/
audited recovery updates on the crossbeam root input. In the 06:31:19.990–08:31:19.990
UTC maker02 window, router polling stalled again at 08:21 and 08:25: retained maximum
953,828 us. Explicit MONOTONIC perf captured 93 and 94 consecutive start_recv ->
run_select -> spawn_per_instance_strategy_threads samples spanning 929.292 and
939.392 ms. The remaining RoutedOrderUpdate select input identifies the private/
recovery lane. Raw evidence lives in hexbot's postwindow_router_20260922/rollout
stall-0821-perf and stall-0825-perf artifacts.

Live private root delivery now uses its own capacity-10,000 poll channel. The old
compatibility receive arm is a never channel in live mode. Cold recovery producers
retain the exact message on capacity/contention; the root consumes private recovery
before execution and public data, yields when a head is reserved but unpublished,
and drains both lifecycle lanes on shutdown. Existing source labels, numeric owner
routing, recovery generation ACKs, deduplication and fail-closed admission remain
unchanged. Paper and existing callers retain crossbeam via the static EventSender
trait. No new thread, allocation, lock, logging call or shared account authority
is added to routing. The router alone writes the private depth high-water scalar;
the existing supervisor exports it asynchronously. High-water is consumer-observed,
not an exact concurrent producer maximum. Crossbeam per-instance/direct-private
and rare worker-status/control lanes are outside this migration; further changes
require separate causal evidence and ordering/backpressure tests.

Validation: all-target locked check, runtime 75 + exchange 907 + engine 137 tests
passed (47 ignored). Actual two-instance root tests cover recovery before execution/
market, duplicate transport, owner isolation and shutdown. Startup buffer and
partially successful audit tests also exercise the live polling transport:
unresolved siblings keep recovery asserted and ACKs stay owner-specific/idempotent.
Targeted recovery 64, owner 1 and metrics 1 tests pass after switching those fixtures.
Existing queue tests deterministically suspend a producer after reservation, prove
immediate empty without overtaking, preserve full-queue records, and cover
disconnect and concurrent ordering.

Sequential release microbenchmark, macOS x86_64, N=100,000 per queue, same-thread
prebuilt u64 push+pop, capacity 10,000, depth 1 and drops 0:

| transport | P50 ns | P99 ns | P999 ns | max ns |
|---|---:|---:|---:|---:|
| crossbeam | 44 | 49 | 53 | 19,241 |
| polling | 43 | 51 | 55 | 23,545 |

This does not establish a speedup or measure wakeup/preemption/end-to-end latency.
The correction is bounded return during interrupted publication. Production
before/after P99/P999, maxima and downstream backlog must be assessed on the new
process after deployment; the previous 120-minute window failed the router check.
