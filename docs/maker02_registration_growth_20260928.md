# Maker02 registration growth boundary

The 2026-09-27 19:27 UTC production interval contained a 3.05 ms reservation
mutation maximum and 3.10 ms registration CPU maximum. These are interval
histogram maxima, not a correlated trace; they do not prove the same sample.
The lifecycle owner's `TokenIndexedRows::rows` was still one flat HashMap of
large retained ownership rows. Inserting across its capacity boundary rehashed
and moved the complete history.

Use a BTreeMap for the owner-local primary row index. Splits move one bounded
node per level instead of the entire retained history. Existing per-token and
per-order GC indexes, identity checks, reservation accounting, immutable
publications and persistence formats are unchanged. Pending physical rebuild
now accepts an iterator so both cold aggregate maps and owner row trees use
the same accounting function.

## Focused before/after measurement

Release build, same local host and compiler/profile, 26,500 retained cancelled
orders followed by 6,000 new registrations, crossing the old 28,672-row growth
boundary. Boundary: complete `register_prepared_order`, including reservation,
routes and persistence enqueue; excludes order preparation, execution-state
publication and transport. Single synchronous owner; queue depth 0, overflow 0.
This is a mechanism benchmark, not an end-to-end production claim.

| Version | N | P50 ns | P99 ns | P999 ns | Max ns |
|---|---:|---:|---:|---:|---:|
| Flat primary row map | 6,000 | 5,149 | 26,636 | 31,125 | 10,232,062 |
| B-tree primary row map | 6,000 | 5,388 | 22,729 | 31,097 | 33,950 |

Run: `cargo test -p hexagent-account --release benchmark_registration_growth_boundary -- --ignored --nocapture --test-threads=1`.

`cargo test -p hexagent-account --release --lib -- --test-threads=1`:
348 passed, 22 ignored (focused benchmarks). Covers replay, terminal identity,
duplicate reservation, failed trade reversal, instance isolation, bounded
mailbox backpressure and GC certification/recovery.

## Ownership and remaining limits

No new thread, shared state, lock, queue or synchronous cross-thread request.
The existing lifecycle owner exclusively mutates these rows. Readers retain
published immutable state. The quote/dispatch path is untouched. Tree nodes
still allocate on the background lifecycle lane; this change bounds structural
work rather than claiming allocation-free private processing. Auxiliary trade
indexes remain hash maps and are a separate profiling/migration target if
private-apply tails grow. Maker02 end-to-end and queue measurements are recorded
with the companion hexbot deployment after its 30-minute observation.
