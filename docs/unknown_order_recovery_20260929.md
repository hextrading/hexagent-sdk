# Unknown-order recovery evidence and cancellation proof retention

Two maker02 placement ambiguities retained risk for 185 and 228 seconds before
expired-market cancellation and complete no-fill history resolved them. Old
HTTP phase records did not retain the target DELETE reason, so they cannot
establish which recovery gate rejected each attempt. The new diagnostics must
be used to distinguish a venue ambiguity from a local recovery failure.

## Per-round evidence

`[order_recovery_round]` runs on the existing cold recovery workers. It records
account/instance/coid/full OID, round timestamp, stage, physical owner role/slot,
elapsed time and bounded detail. Lookup rounds include both owner results;
cancel/audit rounds report eligibility, identity, transport, exact-ACK
schema/target reason, history pages/results and completion. The history commit
path identifies invalid ownership, unreconciled failed trades and superseding
owner state. Separate lookup and cancel/audit rounds correlate by coid and time;
round IDs are not HTTP attempt IDs.

Only the target reason and known error fields are formatted. Detail is UTF-8
bounded to 768 bytes and debug escaped; no authentication headers, signed
request or unrelated order body is emitted. The existing asynchronous appender
retains its bounded/drop-counted policy. Logging has no economic authority.

## Confirmed recovery defect and repair

A deterministic regression reproduces this sequence: exact DELETE succeeds,
historical audit times out, the next DELETE only reports an ambiguous absence.
Previously the first positive cancellation fact was discarded, so the order
remained unknown even when the second history request could have completed.
This reproduces a code defect; it does not prove either earlier live incident
followed that sequence.

After an exact, schema-checked DELETE acknowledgement, a cold typed command
revalidates the complete order root on the existing lifecycle writer: account,
instance, coid, OID, token, side, slot/generation, quantity, price and fee basis.
It records the existing `Cancelled` plus pending-audit state, with the full
residual reservation unchanged. Stronger fills, terminal audits and rejections
win. No new account field or persistence schema is introduced.

Following rounds reuse this positive cancellation fact and retry the complete
historical audit; they do not require another successful DELETE. Startup also
resumes it before market expiry, except for query-repair rows whose terminal
state is not trusted. A stale LIVE lookup cannot reopen the order or supply a
terminal zero-match audit. Final zero-fill release still compares ownership and
all fill/trade obligations atomically on the lifecycle writer. Partial fills,
incomplete history, unreconciled failed trades and newer owner state retain risk.

The state is persisted through the existing asynchronous lifecycle WAL. Once
flushed, replay resumes the pending audit. A crash before the WAL flush can
lose this latest confirmation and fall back to unknown; it cannot turn that
loss into a reservation release. No synchronous fsync is added to a critical
lane. A full/disconnected owner lane returns an error, preserves the unresolved
order and permits a later retry.

## Ownership and bounds

All new transient state lives on the cold recovery stack. The only new message
uses the existing 16,384-capacity lifecycle command lane and its sole writer;
private lifecycle ordering and priority remain intact. The existing 65,536
persistence/mirror lanes batch owner-authored deltas. No new thread, shared map,
queue, affinity role, quote-path operation or shared admission gate is added.
HTTP requests continue through the originating instance's Cancel/Reconcile
actors and bounded mailboxes. No resubmission or account-wide cancel is added.

The account lifecycle facade is an existing migration boundary. This change
reuses its owner-local terminal/audit transition and immutable cold reads; a
future migration can route the same identity-checked confirmation into each
StrategyAccount's own message lane without making cold recovery a state writer.

The exact ACK predicate remains unchanged. `null`, not-found, ambiguous
"canceled or matched", malformed replies, HTTP success alone, or elapsed time
do not establish cancellation. The repair does not guarantee early closure
when the venue never supplies a positive terminal fact.

## Validation

The old implementation fails the new cross-round regression. The repair covers
that sequence, reservation-preserving persistence/restart and duplicate replay,
identity/slot isolation, stronger late-fill audits, bounded lifecycle-lane
overflow, stale LIVE and live-market startup resumption. Existing owner tests
retain ambiguous/malformed replies, incomplete/FAILED/MATCHED history,
HTTP-lane overflow/disconnect and sibling isolation coverage.

Full suites and focused cold-path benchmark results are recorded with the
maker02 rollout evidence in the hexbot repository. Benchmark boundaries are
first owner GET through no-fill audit and owner commit, using mock replies;
they make no claim about venue network latency. Production latency and queue
checks require the completed observation window.
