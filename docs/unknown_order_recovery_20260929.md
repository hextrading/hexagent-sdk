# Unknown-order recovery evidence

Maker02 had two placement ambiguities which retained risk for 185 and 228
seconds, then resolved with expired-market cancellation and complete no-fill
history. Existing HTTP phase records did not retain the target DELETE reason,
so they could not explain each failed recovery attempt.

The diagnostic change adds `[order_recovery_round]` on the existing cold recovery
workers. It records account/instance/coid/full OID, operation round timestamp,
stage, physical owner role/slot, elapsed time and bounded detail. Lookup rounds
include both owner results; cancel/audit rounds report eligibility, identity,
transport, exact-ACK schema/target reason, history pages/results and completion.
The history commit path identifies invalid ownership, unreconciled failed trades
and a compare/commit superseded by newer state. Separate lookup and cancel/audit
rounds correlate by owner/coid and timestamps; their IDs are not HTTP attempt IDs.

Only the target order's reason and known error fields are formatted. Detail is
UTF-8 bounded to 768 bytes and debug escaped before logging; no authenticated
headers, signed request or unrelated order body is emitted. The existing async
appender retains its bounded/drop-counted policy. Diagnostics have no economic
authority and appender pressure cannot clear or alter an order.

No terminal rule, timeout, retry cadence or reservation changes. Added mutable
state is stack-local timing on the existing cold worker. No new shared state,
queue, thread, affinity role, quote-path formatting or cross-thread gate.
The existing private lifecycle and recovery transport ordering/overflow remain
unchanged. Validate live P99/P999 and diagnostic drops during observation.

Validation: two bounded/escaped/target-only diagnostic tests and nine existing
owner recovery tests pass (one benchmark ignored). The owner suite covers exact
ACK plus history, ambiguity retention, wrong owner/slot, unavailable/full lane,
late private advancement, backoff and sibling isolation. Live evidence and any
resulting behavioral repair are a subsequent step, not claimed by this change.
