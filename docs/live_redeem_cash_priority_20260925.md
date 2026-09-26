# Current wallet redemption before historical shortfalls

A wallet calibration can contain an old missing payout and a newly settled redemption. Previously the fallback sorted all negative reconciliation positions by condition ID and spent the common unallocated cash budget in that order. A new $80 payout for `z-new` could therefore credit `a-old`, leaving the currently redeemed position outstanding (even across different strategy owners).

The cold wallet owner now identifies removals without a pre-existing negative reconciliation residual. When this observation's cash increase funds the entire eligible fresh cohort, those candidates are evaluated before historical candidates. Existing binary-outcome, ownership, pending-trade/maintenance and available-cash checks still apply. No cash is manufactured, no unresolved position is deleted and no startup migration is performed. Historical discrepancies and observations without a fully funded fresh cohort retain the existing recovery policy; transaction-level historical repair remains separate.

Insufficient-cash diagnostics emit one warning per calibration pass, retaining the affected-condition count, total expected payout, remaining cash and four condition samples. This bounds repetitive formatting without hiding the shortfall.

All new collections are temporary and owned by the existing cold wallet worker. No strategy/dispatch field, cross-thread message, queue, worker or affinity changes. The existing legacy cold account transaction still owns calibration and publishes its existing asynchronous economic deltas. Moving its remaining aggregate diagnostic formatting outside the transaction is a separate migration; the quote path is unchanged.

Validation: the two-owner regression failed before the change (new owner cash 50 instead of 130), passes after it, preserves the old reservation/position, rejects duplicate credit and survives durable restart. `cargo test --locked -p hexagent-account --lib`: 332 passed, 15 ignored. This includes trade/maintenance interleaving, delayed outcomes, partial scopes, idempotence, persistence overflow, replay and instance isolation.
