# Restore ended-order proof before account reactivation

Maker02 startup on 2026-09-20 revealed two durable recovery gaps. An inactive
ETH owner had never recorded its market's end, so an absent old order could not
enter historical recovery and blocked the requested cash migration. BTC01 had
an absent order whose only matching authenticated trade was FAILED and already
compacted into its durable ledger; history classified this as a possible fill
and repeatedly downloaded unrelated account history.

Recovery now resolves a condition from unambiguous durable token scopes or
settled references. If local end evidence is missing, exact CLOB condition/token
metadata must say closed=true and accepting_orders=false. That only establishes
event end: order absence and complete authenticated trade history are still
required. The history keeps the market filter on every cursor page.

A complete failed-only history is usable only when every referenced failure
has the same durable account/instance/order/token/side root and is already
reconciled or compacted as a terminal FAILED record. Unknown, successful,
nonterminal, incomplete, malformed, or wrong-owner evidence retains the
reservation. The existing sole lifecycle writer rechecks the unchanged order,
zero matched quantity and empty terminal obligations before an idempotent
zero-fill terminal commit. Orphans without an order root remain unresolved.

Read-only live evidence: the ETH market is explicitly closed with matching
binary tokens; one complete history page contained 30 trades and zero matches
for the old order. BTC's one complete market page contained 11 trades and one
matching FAILED record, matching the durable failed tombstone. No production
ledger file was edited or replaced to obtain this proof.

No quote-path change, new runtime field, lock, queue, worker or affinity role.
Metadata/history I/O and cold ledger reads use existing startup/recovery workers;
private lifecycle ownership and overflow behavior remain unchanged. Historical
pagination stays bounded at 64 pages. This is a recovery correctness change,
not a claim of improved steady-state end-to-end trading latency.

Tests cover exact market/token validation, conflicting durable scope, cursor
completion/duplication, mixed and unknown statuses, missing local failure proof,
compacted failures, owner isolation, idempotence, and private fills racing history.
The existing parser allocation tests also had competing process-global counters;
a test-only guard serializes their measurement intervals without adding a
production lock or changing parser behavior.

Validation: account 319 passed / 12 ignored; exchange 895 passed / 35 ignored
(both serial and default parallel runs); engine 131 passed / 6 ignored. No test
failures remain. The two allocation cases also pass in isolation.
