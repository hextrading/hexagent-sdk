# Polymarket taker execution principal

A V2 taker trade's top-level `price` can be the order limit rather than its
execution VWAP. On 2026-09-16, a 15-share SELL reported `0.16` while the matched
legs were 3.27 shares at `0.17` and 11.73 at `0.16`. The confirmed collateral
principal was 2.432700, VWAP 0.16218, and collateral fee 0.142670. Booking the
top-level price understated principal by 0.032700.

For new executions, the private-delivery owner validates and reduces at most
64 borrowed maker legs into quantity and gross collateral. Same-token matching
requires opposite sides. Different-token matching requires equal sides and an
immutable registered binary-condition pair, then uses `1 - maker_price`.
Missing legs, duplicate normalized order ids, inconsistent total quantity,
invalid prices, unknown binary pairs, and capacity overflow fail closed; no
prefix is truncated and no missing execution evidence falls back to limit price.
Missing fee metadata remains separate: the prior published curve, otherwise the
explicit current BTC-crypto adapter fallback 0.07 / exponent 1, is usable without
waiting for metadata.

The private owner selects one `FrozenTradeExecution` containing canonical price,
gross principal, raw venue price, selected fee basis, and fee amounts. Its first
strategy message contains that exact price and `TradeFee`; the existing
asynchronous cold command carries the same scalar certificate. Later metadata
cannot change that trade's amounts. Authenticated WebSocket, gap replay, and
terminal REST recovery use this same private lane. REST callers wait only on
background tasks, with a bounded timeout. Their captured recovery certificate
is checked at dequeue; a changed scope is retried rather than silently joining
another recovery generation. Recovery-tagged updates retain the existing owner
ACK barrier.

For new V2 executions, fees use the selected curve at VWAP and truncate to five
collateral decimal places. A narrowly bounded floating-point integer-boundary
correction avoids truncating a representational value such as
`14.4 / 15` one quantum too low. Twelve individually verified current BTC-crypto
receipts support this convention. The exchange contract accepts an operator
fee amount; these samples do not establish a universal rule for every market.
Summing independently calculated per-leg fees is **not** the implemented rule:
one receipt disproves it despite a coincidentally matching aggregate.

Existing rows with no execution-pricing provenance keep their booked price,
selected curve, currency, and exact stored fees. No ledger migration reprices
old economics or revives settled inventory. Duplicate/finality replay is
idempotent and FAILED reverses the same frozen booking. Persisted active rows
and retired tombstones validate execution provenance. Unknown historical
liquidity role is an explicit startup error; it is not guessed from key shape.
A known historical role with pending attribution follows the exceptional repair
path below.

The per-account private owner owns a preallocated 32,768-row cache. Only terminal
rows with cold completion proof and completed exceptional owner delivery may be
reclaimed. Nonterminal and unacknowledged rows cannot be evicted. A preallocated
atomic completion slot and monotonic ticket identify each cold completion, so a
full advisory dedupe-ACK lane cannot leak cache capacity or let an old ACK reclaim
a reused slot. Scanning completion slots is bounded and occurs only when full
without a reclaim hint; its stress latency is reported separately.

A retained historical trade whose attribution was incomplete at startup can
obtain its frozen certificate asynchronously from the cold owner. At most one
repair batch is in flight and it owns the one-slot reply credit. Its original
replay completion is held until the owning strategy update is enqueued. The
complete retained owner proof survives retired order indexes, reconnects, and
strategy-output backpressure. Later FAILED / CONFIRMED states advance the proof;
a terminal certificate never regresses. Failure keeps recovery closed and the
next gap can retry the exact pending owner delivery. This exceptional branch
does not make normal new fills wait for the cold ledger.

No quote, order-submission, or cancellation code is changed by the reduction.
No new worker, socket request, quote-path lock, quote allocation, or synchronous
ledger read is added to those paths. Private processing does perform additional
bounded validation, so its measured CPU cost must not be described as zero or
as proof of unchanged end-to-end tail latency. Existing synchronous recovery
control/ACK machinery and rare durable identity fallback remain migration debt;
this change does not move ordinary private delivery behind those cold steps.

Validation separates (1) receipt/principal/fee, legacy/WAL/restart/FAILED and
instance tests; (2) real fast-route and cold-certificate consistency, metadata
races, alias identities, overflow and repair/barrier tests; and (3) focused
before/after private-route and full-cache stress measurements. The benchmark
boundary starts from an already parsed event and ends at produced owner updates,
with legacy/current helpers held constant. It excludes socket receipt, JSON
parsing, queue wait, strategy ACK, exchange HTTP, and persistence. Production
observation must measure those stages independently.

On the final development-profile SDK binary, 100,000 already-parsed two-leg
first-seen events measured legacy P50/P99/P999/max of
23.456/65.860/87.995/159.963 microseconds and new route
36.287/93.609/121.559/272.107 microseconds. The legacy route body is copied from
10651046 with current fixtures/helpers, isolating this route change rather than
comparing whole historical binaries. A separate 2,000-sample full-cache
completion-scan stress boundary reached 2.534 milliseconds. Neither result is
an optimized production or quote-path end-to-end measurement. The preallocated
hash-table payload budget is about 17.5 MiB per physical account, in addition to
hash control, completion slots and free/reclaim queues.
