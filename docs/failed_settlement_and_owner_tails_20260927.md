# Failed settlement retry and account owner tails, 2026-09-27

Four maker02 fills reported FAILED before later CONFIRMED settlement. The older
binary made FAILED immutable in the private dedupe, live position, strategy
position and durable account layers. Authenticated REST and decoded on-chain
OrderFilled/Transfer receipts agree on the same order identities and net cash:
0.964320, 0.298500, 2.480290 and 0.480000 (total 4.223110).

CONFIRMED now supersedes FAILED once. Earlier MATCHED/MINED/FAILED events cannot
reverse a confirmation. Frozen execution notional and fee provenance survive
retry. Rebooking also checks the parent cumulative quantity limit. A settled,
zero-value SELL whose virtual inventory was already removed uses a compensating
redemption root on the cold account path; it does not leave negative inventory.
An announced outcome with inventory still present follows ordinary booking.
Other incomplete settlement proofs remain fail-closed. A retired failed trade
requires explicit receipt-backed correction rather than being accepted silently
as an already-booked terminal replay.

`FailedSettlementCashRepair` is offline-only. It checks exact retained trade and
parent audit identity, cash baselines, funded unallocated cash, zero physical and
virtual inventory, zero outcome value, and absence of pending settlement. It
validates a candidate state before publication, preserves physical cash, changes
the tombstone to CONFIRMED and roots the net cash attribution. Duplicate repair
is a no-op after restart. It deliberately does not guess winning-outcome payouts
or attribute unrelated batch payments.

New-order lifecycle registration now uses the existing bounded owner mailbox to
add that order's reservation, rather than scanning all retained orders through
the historical backfill API. A private event overtaking registration preserves
the newer row; identity conflicts and missing routed rows fail closed. The
terminal audit uses the existing per-order trade index instead of scanning every
retained trade. A new prewarmed `polymarket.account.owner_register_order` stage
measures owner registration plus the execution command.

Repeated null order lookups with ambiguous cancellation get a 500 ms per-order
backoff. They retain reservations. This reduces repeated recovery HTTP work; it
does **not** prove an absent order is terminal or guarantee recovery before market
expiry. Positive exact cancellation plus complete history remains required for
the existing active-market recovery path.

## Ownership and verification

- No new threads, mutable global maps, queues, or quote-path I/O/formatting.
- Lifecycle state belongs to the existing account lifecycle owner; strategy
  positions remain strategy-owned. Existing private/lifecycle bounded lossless
  mailboxes, ordering, backpressure and routing identity are preserved.
- The rare settled-retry correction uses the pre-existing cold account fallback
  and full persistence transaction, outside quote processing. It is not evidence
  that all aggregate-account synchronization has been removed; incremental
  migration continues through owner messages and typed mirror persistence.
- Account suite: 344 passed / 19 ignored, plus the final outcome-before-burn and
  redeemed-retry cases passed after the last guard refinement.
- Exchange suite: 930 passed / 42 ignored, including real private-route replay,
  queue overflow/recovery, instance isolation and null-query backoff cases.
- Release registration benchmark: 2,000 events, 26,500 retained orders. Time from
  entering registration/backfill through route/mirror publication, excluding
  preparation and HTTP; inline owner, queue depth 0, overflow 0.
  P50/P99/P999/max: old 295.693/429.117/541.592/644.623 us;
  new 2.948/5.340/15.717/17.083 us. Local macOS host, not maker02 end-to-end latency.
- Terminal audit lookup: 10,000 events, 3,075 retained trades, queue depth 0,
  overflow 0; old P50/P99/P999/max 1.511/1.628/2.834/25.122 us,
  indexed 0.111/0.124/0.146/21.083 us. Hash iteration affects the baseline
  lookup position; a prior run measured a 5.254 us baseline median.
  This boundary covers coverage lookup only, not persistence or HTTP.

Production deployment and fresh latency measurement are necessary before
claiming an end-to-end P99/P999 improvement. HTTP reused-connection tails and
market canonicalization age are separate measurements, not cured by these
account owner changes.
