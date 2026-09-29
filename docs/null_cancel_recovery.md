# Ambiguous cancellation: one delayed null retry

For live per-instance orphan recovery, an ambiguous cancel is queried once. A literal JSON null from an authenticated successful order GET schedules one retry 500 ms after the first reply. The second query excludes the first physical Reconcile slot. Two nulls permit a policy-inferred Cancelled transition if the lifecycle owner still has not received Filled. There is no 1.5-second delay or third query.

Unknown placements must first receive an ambiguous exact-order DELETE reply. A failed/absent DELETE, transport error, 404, invalid response, wrong connection owner, or non-null result does not count as a null; ordinary recovery remains available. Positive lookup evidence is handed to the existing reconciliation path without querying it again. No POST is repeated.

This is a user-selected inference rule, not an exchange cancellation/no-fill certificate. A later fill remains possible after the inferred cancellation has released budget. Exact coid/oid/instance/slot identity, private trade attribution, fee accounting and deduplication remain intact. The durable inferred_cancel flag prevents restart or partial late fills from rebuilding the inferred cancelled residual. A genuine Filled order audit supersedes the flag; late full fills update Filled, and settlement failure/reconfirmation still reverses/rebooks exactly once.

## Ownership and backpressure

The existing pinned per-instance cold recovery worker owns a preallocated 128-entry attempt table and timer. No retry sleeps, new workers, global maps or new queues are added. Deadlines are driven by recv_timeout and processed before the next mailbox command, independent of quote cadence. Full tables preserve the prior recovery/reservation behavior and emit a capacity diagnostic. Missing identity/owner state aborts that attempt.

Before each GET and after its reply, recovery reads the current lifecycle owner through its existing bounded command lane. Reporting snapshots may lag private Filled and are not used as retry authority. The final inferred-cancel commit revalidates exact ownership and Filled precedence in one owner turn. These synchronous calls occur only on the cold worker; the quote path never waits on them. Private updates and timer results use the existing lossless per-instance execution-update channel. Existing legacy lifecycle mirroring is retained; strategy-local reservations remain mutated only by the strategy thread receiving the result. The typed policy sentinel is distinct from an authoritative exchange audit.

## Validation and measurement boundaries

Tests cover the 500 ms deadline, exactly two GETs, no retry after Filled before/during a query, unknown-placement DELETE eligibility, error reset, table/owner-queue overflow, ownership isolation, delayed full/partial fills, duplicate/out-of-order MATCHED/MINED/CONFIRMED, FAILED/reconfirmation and durable restart. Existing account, recovery and strategy suites are also run.

Production acceptance compares the prior 45-minute window (09:26–10:11 UTC on 2026-09-29) with the new restart: order/coordinator queue count/high-water/overflow, GET retry gaps, pending-to-terminal intervals, HTTP acknowledgement and private-application latency N/median/P99/P999/max. The policy adds a deliberate minimum 500 ms wait for null evidence; it is not a quote-path latency optimization. Before deployment, no production after-measurement is claimed. Persistent non-null HTTP failures remain outside this inference rule.
