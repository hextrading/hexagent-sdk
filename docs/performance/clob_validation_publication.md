# Publish CLOB books when validation finishes

The maker02 2026-09-28 09:57–10:42 UTC observation exposed a separate CLOB
publication boundary: a 50 ms tick/BBO quiet interval could extend beyond
100 ms. The ordinary market-receipt-to-callback histogram starts later for
canonicalized books and must not be used to hide this hold.

## Corrections

- A zero-size deletion or ignored invalid-side entry cannot introduce a new
  tick requirement. If a fine-grid level is inserted and subsequently deleted,
  publication can resume once every surviving level fits the confirmed grid
  and the advertised BBO matches the local book.
- A frame that finishes an existing BBO validation publishes the validated
  snapshot immediately even when that final frame leaves the top unchanged.
  It no longer falls through to the 250 ms quantity coalescer.
- Quantity changes at an already-known fine-grid price, with unchanged and
  matching BBO evidence, retain the original tick grace deadline. They cannot
  keep extending it. A newly introduced off-grid price, changed advertised BBO,
  or unresolved depth still receives the existing quiet interval.

The 50 ms grace constant is unchanged. No grid is inferred from a price, no
order parameter changes, and mismatched or crossed depth remains withheld.
Surviving off-grid levels remain protected when a newer on-grid delta arrives.
An authoritative tick-size event still precedes the fine-grid snapshot it
releases. Existing expiry/repair behavior remains in force; this change does
not claim to eliminate all legitimate validation or upstream network latency.

## Empty sides

An empty bid, ask, or both sides is a valid full-book state. Deleting the last
level, with matching venue BBO boundary evidence, publishes immediately. Down
books preserve empty-side semantics when mirrored to Up. The strategy's scalar
L1 cache replaces the absent side with `None`, rather than preserving its old
price. A standalone `best_bid_ask` with a missing/boundary side still cannot
produce a two-sided `QuoteTick`; authoritative book/delta updates carry empty
depth. This distinction must remain explicit in latency investigations.

## Ownership and boundaries

The existing CLOB owner retains all mutable state. Two booleans live in the
already bounded per-frame stack scratch; no worker, queue, shared account,
cross-thread request, or I/O is introduced. A whole-depth grid scan occurs only
when an off-grid insertion or existing tick wait needs revalidation. The common
quantity-only lane stays unchanged, including its zero-allocation test. Existing
snapshot construction still allocates its independently owned depth; removing
that legacy allocation requires the separate bounded snapshot-pool migration.

The regression cases fail on the prior implementation and pass after correction.
They cover deleted/removed fine-grid levels, ignored entries, unchanged-top BBO
completion, duplication, tick ordering, and single/double/mirrored empty sides.
The existing market suite supplies reconnect, overflow, quarantine and market
isolation coverage.

`benchmark_clob_validation_publication` replays the same deterministic schedules
before/after. It reports N/P50/P99/P999/max for two **separate** boundaries:
resident input copy + parse + application elapsed time, and virtual time from the final
confirmation frame to book publication. The latter is a policy-delay simulation,
not measured network or wall-clock end-to-end latency. The benchmark label ending
`_cpu` uses an elapsed wall clock and includes preemption, not thread CPU time. Parser construction,
seeding and earlier frames are outside both clocks; queue depth/overflow are
zero in this single-thread replay. Live queue depth, overflow, publication holds
and downstream latency are checked independently after deployment.

## Linux release measurements

Same maker02 CPU 0/1, nice 19, pinning/FIFO disabled for the isolated replay;
1,000 samples per case before and after. Each row reports P50/P99/P999/max.
The baseline is SDK `4ff01cc8e57fb3b078a7c3fa4a2858065397867a` with only the
benchmark module added. Measurements ran after the prior live observation ended.

| Case | Before copy/parse/apply elapsed µs | After elapsed µs | Before → after virtual additional wait ms |
|---|---|---|---|
| Off-grid deletion | 3.573 / 3.743 / 8.454 / 16.768 | 3.721 / 3.920 / 11.788 / 16.413 | 50 → 0 |
| Unchanged-top BBO confirmation | 2.385 / 2.497 / 3.194 / 7.900 | 2.724 / 3.007 / 11.591 / 12.712 | 249.8 → 0 |
| Known fine-grid quantity update | 2.220 / 2.318 / 2.788 / 7.189 | 2.320 / 2.518 / 10.408 / 11.037 | 50 → 10.1 |

Virtual wait P50/P99/P999/max are identical within each deterministic case.
The last case confirms at 40 ms after seeding, 39.9 ms after the first off-grid
frame; the remaining 10.1 ms honors that frame's original 50 ms grace. These are
policy-delay reductions, **not a claim of faster parsing**: measured parse/apply
cost increases modestly and its observed upper tail includes preemption. Queue
depth and overflow are zero in both single-thread runs; live downstream queues
and private/position latency still require the new deployment's observation.

Local market suite: **111 passed, 13 ignored**, including zero-allocation quantity
processing, empty sides, quarantine, ordering, reconnect and overflow. The first
Linux full Polymarket run passed 472 tests with 28 ignored and failed one existing
private-replay test. That test submitted an asynchronous identity repair and
immediately assumed publication had completed; it passed in isolation. The
fixture now uses the existing test-only owner barrier and asserts the repaired
identity before replay. It changes no production private-event behavior. Final
Linux release suite: **473 passed, 28 ignored**. The repaired fixture passed
**20 consecutive isolated release runs**. The original failure and isolated
pre-fix pass remain in the evidence.

Raw evidence is in hexbot's local
`docs/evidence/maker02_clob_release_20260928T1025Z/`, including original failed
runs and source hashes. The after benchmark predates the test-only barrier edit;
production CLOB source is identical. A discarded local prototype included parser
construction in the timer and was interrupted; it is not used in these results.
