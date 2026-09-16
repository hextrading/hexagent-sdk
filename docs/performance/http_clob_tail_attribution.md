# HTTP phase attribution and CLOB frame ownership

Business HTTP attempts previously had completion latency but no durable join to
successful socket-phase measurements. Record each physical attempt with its root
attempt ID, cancel alternate-leg number, role/slot, runtime queue wait, socket
wait, DNS/TCP/TLS/TTFB/body timing, response processing, peer and connection
generation. This changes neither admission, concurrency, retry nor timeout policy.

The I/O coroutine publishes one fixed-size record (at most 256 bytes) to the
existing account execution-audit FIFO (4096 entries). The existing background
worker owns formatting, phase histograms and buffered writes, flushed at one
second intervals and shutdown. FIFO ordering applies to accepted publications;
interleaved requests join by account and attempt ID, not adjacent lines. This
advisory lane is separate from lossless private/lifecycle lanes. Full/disconnected
queues drop telemetry only, count `phase_dropped`, and never delay execution.
`queue_high_water` is a sampled shared-lane occupancy estimate, not an exact peak.
Missing observations remain unknown. No worker or affinity policy is added.
Prewarm/heartbeat paths outside `execute_http_on` retain their existing metrics.

CLOB price-change output is already canonical and coalesced. Its owned Vec now
moves into an empty frame batch; nonempty array batches retain the existing
newest-wire-order merge. This removes one redundant output-buffer allocation for
the measured frame. Existing allocations inside parsing/book production remain;
future removal requires owner-local reusable buffers and profiling.

The existing Linux perf ring now freezes on either price-change apply >=5 ms or
whole read handler >=10 ms. Its capacity remains 2 and cooldown 300 seconds, owned
by the CLOB reader. The existing background sampler/replay worker is reused.
Thread CPU timing now ends at the same parse/apply boundary as wall timing;
resource-counter deltas still explicitly span multiple frames. A residual tail
is not evidence of allocator contention without a matching captured stack.

## Validation

macOS, debug test profile, System allocator, serial paired measurements with
alternating old/new order. These are focused local costs, not production latency
or an estimate of exchange-network improvement. Times below are nanoseconds.

| Boundary / version | N | P50 | P99 | P999 | Max |
|---|---:|---:|---:|---:|---:|
| Inline HTTP phase histograms / old | 100000 | 1807 | 1904 | 16238 | 65423 |
| Arc clone + phase try-send / new | 100000 | 209 | 242 | 314 | 26814 |
| Decoded apply + batch assembly + output drop / old | 20000 | 59277 | 109032 | 180779 | 248119 |
| Same CLOB boundary / new | 20000 | 58630 | 101886 | 170998 | 297921 |

HTTP dequeue/export are excluded; queue HWM 1, drops 0. CLOB input decode/reset
are excluded; no queue is involved. CLOB thread CPU P50/P99/P999/max changed from
58590/107798/177788/246933 to 57946/100693/169895/238876 ns. Total allocation counts
were 200000 -> 180000, bytes 58640000 -> 34960000. Wall maximum regressed in this
sample even though measured percentiles and CPU maximum fell; live tails require
separate observation.

`cargo test --locked -p hexagent-exchange --lib -- --test-threads=1`:
831 passed, 29 ignored. Coverage includes ordered root/alternate association,
bounded overflow/disconnect, account isolation, actual local HTTP success/error/
invalid JSON and reconnect generation, CLOB coalescing/order/overflow/replay,
and whole-handler trigger selection. The two ignored benchmarks above were run
explicitly. A sampled full-suite timeout was in test-only `clear`: writing None
to all 65536 ArcSwap slots repeatedly paid reader debts. Sparse clearing fixes
the fixture without relaxing the two-second barrier or changing production code.

Production validation must additionally verify phase/business joins and drops,
all downstream queues, exact process identity, and market-to-ack P99/P999 over a
fixed observation window. Percentiles from different stages must not be added.
