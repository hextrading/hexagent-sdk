# maker02 background tails and stage attribution

The 2026-09-27 05:40:36–08:39 UTC live window had a 23.65 ms background
registration maximum, 22.42 ms lifecycle queue maximum, 190.09 ms cold account
lock maximum and 1.233 s place HTTP maximum. The deployment used SDK `e969ec7`.
Minute histogram maxima alone cannot identify the instruction responsible for
an individual incident. Focused benchmarks reproduce two history-dependent
costs; the new observations distinguish the remaining stages on the next run.

## Changes

* Execution identity snapshots use a 64 × 64 immutable directory. One update
  clones 128 Arc pointers and one small leaf, instead of hundreds of retained
  String keys/values in a flat 64-shard map. All three identity maps still
  publish in one execution snapshot. Old readers and unrelated leaves remain
  immutable. This reduces average leaf size, not a hard bound on hash collisions.
* Split/merge reserve, failure and confirmation persist a diff of their own
  economic/operation projection. They no longer clone the complete account,
  including retired trade ownership, before and after each mutation. The
  projection includes cash, the two tokens, maintenance coverage, operation
  state, reconciliation residuals, uncertainty, risk blockers and cold counters.
  Unchanged lifecycle leaves are not written. WAL format, writer, admission
  flush, overflow/failure gates and replay rules are unchanged.
* `owner_register_ledger` and `owner_register_publish` split the existing total
  registration clock on the background lifecycle owner.
* Public CLOB owners publish compact timing observations to preallocated
  65,536-entry `TryQueue`s. The existing `latency-dump` worker bins, computes
  percentiles and logs them. Saturation/contention drops only telemetry;
  `[latency_observation_queue]` exposes capacity, depth, high water and drops.
  Queue initialization occurs on CLOB-runtime/Polymarket-feed startup only.
* HTTP observes application plaintext reads/writes after TLS, and Linux
  TCP_INFO on the owned socket at first write/first read. Fixed timing values
  travel through the existing 4,096-entry advisory HTTP audit queue. The
  existing audit worker formats/writes `http_phase` records; its drop/high-water
  counters and request/attempt/account/slot/generation identity are retained.

No strategy account ownership, private-event routing, order retry, reconnect,
CPU role/affinity or business queue capacity changes. No new thread. The quote
callback and dispatch code are unchanged. Broad cold transactions outside
maintenance still use before/after capture; migrate those separately with
scoped replay-equivalence evidence rather than enlarging this projection.

## Observation boundaries

New names below are prefixed by `polymarket.ws.`:

| Stage | Boundary |
|---|---|
| `clob_source_age_at_publish` | Wall time before bridge enqueue minus book/quote local timestamp; preserves its existing semantics |
| `clob_bridge_queue` | Monotonic bridge enqueue to ordered dequeue, including retained pending envelopes |
| `clob_quote_wait_tick` | Pending quote receipt to release when tick metadata arrives |
| `clob_quote_wait_deadline` | Pending quote receipt to deadline release |
| `clob_bbo_wait_ready` | First pending BBO observation to consistency resolution |
| `clob_bbo_wait_deadline` | First pending BBO observation to deadline resolution, possibly repair rather than publication |
| `clob_bbo_wait_snapshot` | First pending BBO observation to authoritative snapshot replacement |
| `clob_deferred_timer_late` | Actual deadline handler entry minus scheduled 50 ms deadline, saturated at zero |

Hold/queue clocks are monotonic. Source age is wall-clock based and is not
synonymous with wire latency. These are distributions, not per-event trace IDs;
do not add their unrelated quantiles or claim an exact historical-event join.
The 50 ms consistency protection and source timestamps are preserved.

New fields in `http_phase`:

| Field | Meaning |
|---|---|
| `first_write_offset_ns` | Request timing start to first successful plaintext write; includes connect/client scheduling |
| `write_span_ns` | First to last successful write |
| `flush_offset_ns` | Request timing start to completed transport flush after a write |
| `first_read_offset_ns` | Request timing start to first observed response bytes |
| `response_wait_ns` | Last write/flush to first observed response bytes |
| `header_decode_ns` | First observed bytes to response headers ready; includes partial-header wait, parser and scheduling |
| `written_bytes`, `read_bytes` | Plaintext HTTP bytes, including headers, not just body |
| `tcp_info_sampled` | Both Linux TCP_INFO samples succeeded on this connection |
| `tcp_rtt_us`, `tcp_rttvar_us` | Kernel smoothed RTT/variation at first response read |
| `tcp_retrans_delta` | Connection total retransmissions between the two samples; valid only when sampled |

Zero timing can mean an unobserved boundary on failure; consult byte counts,
`incomplete_phase` and sample validity. `response_wait_ns` includes network,
remote handling and local runtime scheduling. It is not pure network RTT or
exchange CPU time. Socket wake-to-poll/kernel receive timestamp instrumentation
is still needed to separate those completely. Existing DNS/TCP/TLS/body/slot
and runtime-queue clocks remain available. No implicit order replay is added.

## Focused release measurements

macOS x86_64, Rust 1.97.1, LTO off, 16 codegen units, serial tests. Nanoseconds,
nearest-rank quantiles; sample collection excludes fixture construction and
warmup. Identity publication includes destruction and checks the following
reader. Maintenance uses the real persistent account and WAL writer, bound cold
and lifecycle lanes, and includes publication in the first boundary. Each
iteration drains WAL before the next; these are local probes, not live E2E proof.

| Boundary | N | Before P50 / P99 / P999 / max | After P50 / P99 / P999 / max |
|---|---:|---|---|
| Three identity maps, 26,500 retained rows | 2,000 | 190719 / 364018 / 470240 / 510598 | 12236 / 20892 / 39015 / 61287 |
| Identity update through following reader | 2,000 | 190925 / 364241 / 470441 / 510845 | 12375 / 21042 / 39142 / 61409 |
| Maintenance reserve, 80,000 retired rows | 200 | 93713267 / 103773097 / 390449295 / 390449295 | 452292 / 880307 / 908379 / 908379 |
| Reserve through WAL flush | 200 | 126559348 / 139982393 / 424975827 / 424975827 | 19391484 / 57947484 / 71136875 / 71136875 |

N=200 cannot establish a stable P999; its nearest-rank P999 equals maximum.
Identity queue depth/overflow = 0/0. Both maintenance runs: 432 WAL records
enqueued and written, writer high water 1, overflow/failure 0; mirror high water
and overflow 0. No unbounded downstream backlog was hidden by the measurement.

Compact observation publish, N=100,000: P50/P99/P999/max = 70/77/134/474 ns;
through dequeue = 113/125/1364/6600 ns; high water 1, drops 0. The latter is an
immediate-dequeue probe, not the periodic consumer's residence time.

Linux Docker x86_64 loopback, release, N=2,000, alternating two reused sockets:
request construction through complete body was 25270/100804/157005/296892 ns
for bare Hyper and 31623/124615/282763/377262 ns for the instrumented client.
Each had one in-flight request, zero queued/overflow. Bare Hyper lacks the
existing request gate, deadline and other phase accounting: this is a transport
reference, not an isolated before/after estimate for TCP_INFO. It verifies the
instrumented path including Linux sampling; Docker tails are not production SLOs.

## Validation and reproduction

Release account tests: 346 passed, 20 ignored. Release exchange tests: 930 passed,
43 ignored. Linux runtime tests: 78 passed, 3 ignored before the additional
ignored loopback probe; all HTTP boundary/timeout/reuse tests passed. Existing
ordering, duplicate delivery, replay, bounded overflow and instance-isolation
tests remain in those suites. Two recovery tests now wait on their existing
lifecycle test barrier before observing asynchronous publication.

```sh
CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 cargo test --release -p hexagent-account --lib -- --test-threads=1
CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 cargo test --release -p hexagent-exchange --lib -- --test-threads=1
CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 cargo test --release -p hexagent-runtime --lib -- --test-threads=1
# Explicit ignored probes (run each alone, --nocapture --test-threads=1):
# benchmark_live_identity_publication
# HEXAGENT_TAIL_SAMPLES=200 ... benchmark_live_maintenance_transaction
# benchmark_observation_publish
# benchmark_http_io_boundaries
```

The full live evidence and benchmark transcripts are retained locally in the
hexbot workspace under `docs/evidence/maker02_restart_20260927T0840Z/` and
`docs/evidence/maker02_stage_fix_20260927/`. No production restart or change is
part of these measurements. Validate the same live E2E/queue boundaries after
deployment before claiming the 23.65/190.09 ms or 1.233 s incidents eliminated.
