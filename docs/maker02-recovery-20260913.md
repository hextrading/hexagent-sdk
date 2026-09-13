# maker02 CLOB recovery fixes, 2026-09-13

The production investigation covered 04:30:29–08:06:28 UTC with SDK
`6bc1f7f665dac25db16e4d7fa0d54157d486e97d`. This change addresses the recorded
allocation stack, subscription-generation mismatch, and ambiguous trade-backfill
telemetry. Production credentials and account response payloads are excluded.

## Findings and behavior

Two captured `price_change_apply` tails lasted 15,051 and 6,251 µs. The final perf
samples entered `alloc::fmt::format_inner` from `ClobLocalBooks::apply_price_change`,
then mimalloc reclamation and `madvise`. Each normal subscribed BBO validation
formatted a diagnostic string before knowing whether a mismatch would persist.
The history now stores four inline numeric samples. Insertion and oldest-sample
eviction do not allocate or format; a finalized mismatch retains the original
timestamps, entry counts, expected/actual prices and missing-field/sentinel
distinction for rendering.

An activated preseeded subset previously rewrote `wire_subscription` although
both connected sockets still carried the preceding union. A healthy standby was
then rejected against a generation it had never subscribed to. Routing now
changes independently of the physical wire set. The current socket pair retains
its truthful generation, while a separate reconnect target records the latest
requested set. A subsequent seeded candidate or cold connection removes expired
tokens. Every newer command cancels the preceding candidate, including already
completed candidates awaiting selection; incoming standby connections are also
checked against the actual wire generation. The seeded-books, freshness and
distinct-peer promotion gates remain in force.

Read-only authenticated trade queries plus the persisted snapshot/WAL verified
all eight warning IDs: nine account-owned legs, 85.65 shares, all CONFIRMED with
matching order, token, side, quantity, price and instance ownership. Seven IDs had
terminal sources in the snapshot. The eighth was in the post-snapshot WAL with
MATCHED → MINED → CONFIRMED → retired economic-source transitions. This evidence
does not establish the exact historical REST response visibility time, but does
rule out final missing economics for these eight IDs at the observation time.

Backfill now distinguishes already-booked nonterminal legs from absence of both
REST and ledger evidence. Pending and received records contain the exact trade
ID. Invalid response schemas, malformed cursors, repeated cursors and exhausted
pagination remain unresolved errors. Lookup follows up to four pages, keeps the
exact trade-ID filter and signs the query-free endpoint. Existing private feed
and scheduled reconciliation own retries; there is no new retry thread or sleep.
Received rows continue through the existing authenticated owner parser and
idempotent accounting path. These telemetry counts cannot release reservations;
the authoritative order audit still decides economic coverage.

## Validation

* Exchange library: 623 passed, 9 ignored (including opt-in benchmarks/live tests).
* Account library: 240 passed, 5 ignored.
* New regression coverage: four-sample history overflow and ordering, 10,000
  allocation-free inserts, missing-field/sentinel fidelity, subset routing,
  current-generation promotion, cold reconnect target, superseded asynchronous
  candidates, exact trade identity, response validation and pagination bounds.
* Existing exchange/account suites cover lifecycle ordering, duplicate/replayed
  private events, instance isolation, reservation coverage and durable overlap.

Focused before/after benchmark, same machine and test allocator:

| Boundary / build | N | P50 ns | P99 ns | P999 ns | Max ns | Allocations | Allocated bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before, SDK 6bc1f7f + benchmark | 100,000 | 96,271 | 137,240 | 192,278 | 339,047 | 2,500,000 | 392,200,000 |
| After | 100,000 | 89,463 | 133,689 | 182,036 | 281,887 | 2,300,000 | 369,800,000 |

Boundary: resident JSON parse + two price-change entries + BBO application + event
construction + batch drop, alternating actual best-ask changes after 256 warmups.
Percentiles use nearest ranks. Samples are measured on one thread; queue high
water = 0, overflow = 0, and no downstream worker can absorb this measured work.
This is **unoptimized macOS test code, Rust 1.97.1, System/counting allocator**,
not production Linux/mimalloc end-to-end latency. The allocation-site removal is
deterministic; the wall-time differences are a local observation, not a live SLA.

```sh
cargo test -p hexagent-exchange --lib -- --test-threads=1
cargo test -p hexagent-account --lib -- --test-threads=1
cargo test -p hexagent-exchange --lib benchmark_clob_bbo_change -- --ignored --nocapture --test-threads=1
```

## Ownership and remaining migration

The CLOB lane owns the inline history and reconnect target. History capacity is
four with oldest diagnostic sample replacement; it is not a private-event queue.
No private/order lane, queue capacity, priority, ordering, affinity class or
worker topology changes. Reconciliation reads existing immutable owner snapshots
on the account's Reconcile connection worker and returns parsed execution events
through the existing owner route.

The general L2 path still performs 23 allocations/frame in this fixture, including
existing per-frame maps, token strings and output vectors. Final error rendering
and diagnostic sampling also remain in the existing lane. The next migration
should replace scratch maps with preallocated token slots and bounded outputs,
then send typed error records to the background diagnostics worker. Removing the
observed formatting call does not prove that every allocator-driven tail has
been eliminated. Post-deployment validation must measure market receipt → quote
decision → dispatch, exchange acknowledgement and private application separately,
with N/P50/P99/P999/max, queue high water and overflow per lane.
