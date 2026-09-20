# Startup cash migration publication and WAL recovery

Maker02's migration log recorded 5,577.804746289961 from eth02 to btc02,
but the next wallet calibration copied pre-migration virtual cash back into
the cold ledger. The physical wallet total remained intact. A copied live
snapshot and its checksummed WAL failed immutable-baseline validation:
btc02 cash 1,120 versus replay 6,697.804746290054.

Recovered-order startup binds the lifecycle owner before migration. The general
control guard intentionally stops replacing bound lifecycle shards, so the
migration's aggregate-only cash update was insufficient. It now publishes only
its cash delta and configured weights to the existing economic projection, as
the cold economic guard already does. Unbound startup retains its existing
bootstrap path. No positions, order maps or trade ownership are replaced.
The operation remains durable and idempotent; reservations, unresolved orders,
trades and maintenance still reject migration. No new strategy-thread work,
queue, worker or shared account admission gate is introduced.

Already affected WALs need a narrow startup repair. A fresh checksummed
migration frame must contain the exact before balances and every changed after
balance. The surviving immutable migration record must match that evidence.
Applying its transfer vector must make the complete persisted-state validator
pass; multiple explanations, a mismatched amount, missing WAL proof, unrelated
economic errors or invalid reservations are rejected. The repair changes only
virtual cash and derived reconciliation, preserves immutable roots, and uses
the existing atomic startup snapshot commit. A migration record in a snapshot
alone cannot authorize another transfer.

Before the fix, the bound-lifecycle regression failed with destination cash 0
instead of 100. Coverage now includes wallet refresh, repeated migration,
restart, original position ownership, a WAL-proven overwrite, an unrelated
one-unit discrepancy, and a subsequent unjournaled snapshot modification.

Validation on a local copy of the live ledger restored btc02 to
6,697.804746289961 and eth02 to 339.8044580000005, leaving physical cash
7,398.774529 unchanged. The eth02 remainder is historical settlement credit;
it retains its original owner. Production acceptance must check these owners
again after the script-based restart. No production ledger is edited manually.

All new work is startup/cold-path only. Critical-path latency and queue evidence
will be collected in the deployment's final 30-minute observation; this change
does not claim an end-to-end latency improvement.

Final validation: account 323 passed / 13 ignored; engine 134 passed /
7 ignored; exchange 900 passed / 35 ignored. Migration-focused tests: 5 passed.
