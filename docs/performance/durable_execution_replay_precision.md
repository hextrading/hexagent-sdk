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
