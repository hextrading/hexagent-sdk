# Exact durable execution replay after JSON restoration

After the maker02 restart at 2026-09-16 17:55:26 UTC, three previously confirmed
taker sells failed replay. Each retained price was `0.98`, gross notional `19.6`,
quantity `20` and collateral fee `0.02744`. Recomputing gross/quantity produces
`0.9800000000000001`: one ULP above the JSON-restored price. The persisted-state
validator accepts that representation, but the frozen DTO validator required
bitwise-equivalent arithmetic. Its later historical fallback reported a generic
durable-proof conflict, leaving an ownership anomaly that blocked next-event
split maintenance and caused the event to skip quoting without inventory.

Keep strict arithmetic validation for new executions. Only when it fails for a
finite actual-notional DTO, the cold account application checks the existing
single retained trade. It must match the complete frozen execution exactly
(price, raw price, gross, fee amount/currency and fee basis), as well as account,
trade, order/coid, token, side, quantity and role. Existing ownership and lifecycle
validation still runs. Unknown or changed proofs remain rejected; original
prices and fees are never recomputed or rewritten by this exception.

The only production caller is the existing cold user-feed account application.
Canonical DTOs bypass the lookup entirely. There is no quote/private-fast-route
lookup, added queue, worker, retry, gate, log or persistence operation. Exceptional
historical lookup uses the existing account-owned/read-snapshot API for one row;
it does not scan historical trades. Normal lifecycle persistence and verified
anomaly recovery remain in the existing owner path.

The regression uses a real persistent account and WAL restart. It reproduces
the rejection before the fix, then verifies duplicate confirmed/older replay,
zero fill delta, exact unchanged trade economics and owner/sibling balances,
and recovery of the specific false anomaly. Additional tests reject a fresh
noncanonical DTO, a one-ULP fee alteration, changed price/gross/basis, foreign
trade/order/coid/token, changed side/quantity/role; valid MINED/FAILED replay
reverses the original economics exactly once despite changed current metadata.

Validation: all 310 account tests passed (10 ignored), including 12 focused
execution tests. The exchange suite verifies integration with private routing,
bounded overflow, ordering, replay and instance isolation. No timeout or existing
risk assertion was relaxed. Live verification must confirm anomaly clearance,
normal next-event split/quoting, unchanged durable economics, and a fresh fixed
30-minute observation after deployment.

## Recovery-aware replay deduplication

The next restart exposed a separate recovery defect: durable lifecycle coverage,
route commit acknowledgements and cold replay caches could all suppress a trade
whose lifecycle was already confirmed but whose ownership/private-event anomaly
still required validation. Four persisted replay anchors correctly retained the
REST range; repeated successful sweeps nevertheless skipped the repair.

Private replay now treats an exact pending anomaly (including maker leg keys) as
requiring the existing cold validator. It bypasses those three cold-work skip
conditions, while retaining strategy-delivery deduplication. Successful parsing
and normal account transition alone clear the exact anomaly; malformed events,
foreign identity and unrelated anomaly keys remain blocked. The previous exact
frozen-execution proof remains required. No operator clearing or economics edit
is involved.

No mutable field or queue is added. Existing immutable anomaly snapshots are
published by cold account transactions; the private route/cold owners read them.
An existing release-published uncertainty flag is an advisory replay hint only;
healthy traffic returns after one atomic read. Existing live/replay lanes remain
bounded at 1024 with live priority and replay acknowledgement/backpressure;
owner-local replay caches retain their capacities and ordering. Quote/dispatch
processing and affinity are unchanged. Further removal of the legacy shared
ledger high-water lookup should publish owner-specific lifecycle certificates,
with the same repair and delivery barriers, rather than adding new shared gates.

Three regressions cover all three dedupe layers, malformed retry, retired trade
replay, duplicate delivery/economics and unrelated instance/anomaly isolation.
The first two failed against the previous SDK before the repair. The full
exchange suite also covers overflow, reconnect/replay and ordering.

Paired macOS debug measurement, alternating exact pre-change helper and new
helper over the same healthy durable replay (20,000 events each). Boundary is
private lifecycle high-water lookup including the anomaly hint, not quote work:
old/new P50 4142/4379 ns, P99 4530/4602 ns, P999 25932/25436 ns, max
78456/65294 ns. Queue depth/overflow 0/0 (synchronous lookup only). This small
local overhead does not establish production end-to-end improvement; live
comparison remains required.
