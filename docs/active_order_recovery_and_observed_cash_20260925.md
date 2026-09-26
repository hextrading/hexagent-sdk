# Active unknown-order recovery and observed cash attribution

Runtime private-feed recovery previously repeated GET for an unknown placement
until expiry when replicas returned null. It now asks the owning Cancel actor to
DELETE the exact OID after absent lookups. Only membership in a schema-checked
`canceled` list, without a contradictory `not_canceled` entry, authorizes the next
step: a complete authenticated historical trade sweep through Reconcile actors.
The existing lifecycle writer then compares the entire ownership identity and
zero-match obligations before committing Cancelled. Associated fills, incomplete
pagination, HTTP errors, disconnected/full lanes, and ambiguous DELETE replies
retain reservations. The API still cannot guarantee fast closure for an order
that was never indexed and receives no explicit cancellation acknowledgement.

No quote-path changes or new workers/queues. This uses the startup-bound bounded
probe/recovery transport; each sequential request awaits one reply on the cold
recovery worker. Cancel and Reconcile actors preserve the originating instance.
An unavailable lane never falls back to a shared socket. Late private events
retain the durable coid/OID ownership mapping and override stale no-fill proofs.
The existing expiry path retains its scope/end-of-event checks.

`attribute_observed_cash_adjustment` is an offline-only accounting operation for
an independently audited cash movement already included in the physical wallet
snapshot. It changes one explicitly selected virtual owner and its durable
external adjustment root; it does not apply the wallet transfer again. It rejects
bound live owners, unsettled economics, changed expected balances, negative or
reserved cash consumption, and unfunded positive credits. The operation ID is
namespaced, survives WAL/restart, and cannot be reused with a different amount or
owner. It does not infer ownership, write off a gap, or inspect chain RPC. A full
cold persistence transaction includes reservations; no new live mutable fields.

Validation: exchange library 922 passed / 40 ignored; account library 333 passed /
15 ignored. Recovery tests cover the full runtime entry point, exact routing,
malformed/contradictory ACKs, incomplete history, unreconciled FAILED or MATCHED
trades, a late trade between audit and commit, duplicates and bounded-lane overflow.
Cash tests cover persistence/restart, duplicate/reassigned operations, stale
balances, funding/reservation limits and rejection of online use.

Focused cold-path benchmark, macOS x86_64, debug profile, mock replies without
network delay, N=100 per mode. Boundary: first order lookup through return; new
mode includes two absent GETs, exact DELETE ACK, complete one-page trade audit and
lifecycle-owner commit. Old mode ends pending after two GETs; its duration is not
successful recovery latency. No claim about production network P99/P999.

| Mode | Resolved | P50 ns | P99 ns | P999 ns | Max ns |
|---|---:|---:|---:|---:|---:|
| Existing GET-only attempt | 0/100 | 26,593 | 178,050 | 180,238 | 180,238 |
| Active cancel + complete audit | 100/100 | 139,072 | 285,989 | 410,425 | 410,425 |

600 total requests, queue capacity 2, observed maximum depth 1, no overflow.
Production uses the existing configured transport capacity, not this fixture's 2.
An exact ACK followed by failed history remains unresolved; retry or the existing
expiry/restart audit is still required. No ambiguous ACK or elapsed-time threshold
is promoted to proof of cancellation.
