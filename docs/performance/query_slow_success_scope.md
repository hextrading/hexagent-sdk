# Keep successful query latency out of order-connection health

The maker02 startup on 2026-09-15 incorrectly applied the order lane's 500 ms
slow-success threshold to successful historical queries. Query requests used
their own four HTTP/1 slots, yet two slow Query slots within 750 ms extended the
process-wide new-placement gate by five seconds. They also requested connection
retirement despite returning valid responses.

The fix applies slow-success retirement and correlation only to Fast and Cancel.
Query, Reconcile and GapReplay successes do not retire a connection or alter
the placement deadline. Actual Timeout/Transport evidence keeps the existing
behavior for every role. Authentication, private-gap and inventory-uncertainty
gates are unchanged. Remaining actionable slow-success warnings retain WARN
severity; `placement_gate_active` now reflects the actual deadline rather than
only the current correlation window's connection count.

## Production evidence before the change

Source artifact: `final_observation/live-713031884.log`, collected for the
2026-09-15 maker02 deployment investigation. The startup began at
15:10:24.391 UTC; successful Query warnings span 15:10:38.637–15:15:12.979 UTC.

| Observed item | Value |
| --- | --- |
| Slow-success calls | 399, all Query |
| Logged elapsed ms: N / P50 / P99 / P999 / max | 399 / 691 / 1138 / 1507 / 1507 |
| Exact measurement boundary | `execute_http_on` start through body/JSON and local HTTP outcome handling, before slow-success health handling; integer milliseconds from the warning |
| Actual successful replacement publications | 36: four Query slots, nine generations each |
| Successful historical no-fill audits | 21, each requiring 19 trade pages; 21 × 19 = 399 |
| Actual connection-failure-cluster / incomplete-request logs during this startup | 0 / 0 |
| HTTP queue depth / overflow | Not measured by these logs; no inferred zero |

Nearest-rank quantiles above are for the observed slow-success warning samples,
not an unconditional HTTP distribution or end-to-end latency distribution.
Existing per-slot 30-second rebuild cooldown explains why 399 retirement
requests produced only 36 replacements. Every replacement logged successful
prewarm before publication. This is unnecessary connection rebuild/prewarm
churn, not evidence that all 399 business requests paid a cold TCP/TLS handshake.

For example, the first reused Query request had zero measured slot wait,
615.821 ms TTFB, 16.230 ms body and 632.051 ms segmented total. After replacement,
the same slot's next logged reuse still took 613.061 ms. Later replacements
also retained the same observed Cloudflare peer. These samples provide no
evidence that replacing the socket cures historical endpoint response latency.

Applying the existing five-second extension rule to the logged correlation
counts reconstructs gate intervals 15:10:39.274–15:15:07.370 and
15:15:08.280–15:15:17.979 UTC (277.795 s combined). These are inferred gate state,
not measured lost trading time: strategy/execution startup followed the final
audit at approximately 15:15:13. The old log's intermittent
`placement_gate_active=false` only meant the new 750 ms correlation window had
one connection, even while the previous five-second deadline was still active.

## Verification and ownership

Focused unit tests replay 399 representative 700 ms successful Query pages over
four rotating slots and assert zero retirement decisions, zero correlation
state and no placement deadline. Additional tests cover all three query roles,
the unchanged 500 ms Fast/Cancel threshold, duplicate-slot evidence, correlation
window rollover while a gate remains active, exact deadline expiry/extension,
and actual query-role failures retaining their gate behavior.

Validation: `cargo test -p hexagent-exchange network_incident::tests --lib`:
10 passed, 0 failed. `git diff --check` passed.

This is a policy correction on the existing HTTP completion path. It adds no
mutable state, queue, worker, lock, allocation or I/O. The existing atomic global
gate and pool ownership are retained; no new strategy-thread access is added.
Its cross-account shared policy remains an existing migration constraint:
future finer isolation should publish explicit account-scoped health events.

Post-change live HTTP and end-to-end P50/P99/P999/max still require the fresh
30-minute deployment observation; no production latency improvement is claimed
from the deterministic gate tests. Replacement counts and queue/overflow
telemetry must be checked alongside those distributions.
