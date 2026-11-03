# Polymarket API and Protocol V2 migration

Reference date: 2026-11-03 UTC. Protocol V2 is separate from CLOB V2:
keep `clob_version = "v2"`. Existing CTF holdings continue using their original
ledger and domain version 2. Do not convert all token IDs to PositionManager IDs.

## Wire and ownership contracts

- Position reads use `/v2/positions`, the `data`/`pagination` envelope,
  `status=OPEN`, `include_archived=true`, `filter_type=TOKENS`,
  `filter_amount=0`, and opaque cursors.
  Every page retains the wallet anchor. Missing fields, wrong wallet, cursor
  cycles, conflicting overlapping rows and bounded-scan overflow fail the
  entire snapshot. An error never publishes an authoritative empty account.
- Gamma's `version` selects `clobTokenIds` for V1 and `positionIds` for V2.
  Missing/unsupported versions, invalid IDs and mismatched binary outcomes are
  rejected before market admission. V2 bytes31 condition IDs are right-padded
  to the internal bytes32 representation; Router ABI remains left-aligned bytes31.
- The signing selector is cached in each immutable `PreparedToken` during
  instrument registration. Every order retains its event's cache, including
  retired-event orders. Live signing reads that cached selector; it performs
  no protocol lookup or token classification. CLI/legacy orders without a
  prepared token validate and classify on their cold compatibility path.
- V2 uses ExchangeV3/domain 3, PositionManager balances/payouts and Router
  split/merge/redeem. Redemption passes explicit integer quantities for each
  outcome. Mixed-ledger balance batches preserve input ordering and fail if
  either ledger read fails. Unresolved payout RPCs remain provisional.
- Journaled deposit-wallet split recovery reuses the exact signed call, nonce
  and body hash. A successful canonical receipt must include the exact wallet
  batch nonce and net delivery of both requested outcomes. Missing journal
  proof cannot trigger a guessed V2 split resubmission.
- Heartbeat cadence is fixed at 5 seconds, with a 3-second request timeout and
  skipped missed ticks; request latency does not extend the next interval.
  Its cancellation scope remains API-key-wide: isolate keys if independent
  cancellation domains are required.
- Explicit restart 425 and `post_only_mode` reject placement. Generic 5xx,
  transport timeouts and uncertain cancellations retain reconciliation.
  Numeric/date `Retry-After` and per-order retry delays travel in existing
  lifecycle messages. Hexbot applies conservative owner-local pauses to new
  placements; cancels/private fills continue. No shared runtime gate is added.

All new scans, ABI work and settlement RPCs run in existing cold maintenance
or I/O contexts. No new production thread, queue or mutable account authority
is introduced. Existing bounded/lossless private lanes retain ordering and
backpressure behavior. Settlement reads are asynchronous and do not enqueue
nested blocking jobs into a pool whose caller may already hold a worker.

## Activation

`approve_v2 --dry-run` reports existing and missing permissions. The approval
plan now includes pUSD → ExchangeV3/Router and PositionManager → ExchangeV3/Router,
while retaining CTF approvals. Actual approvals, deployment and funded BUY/SELL
or split/redeem checks are operator actions; this migration does not run them.
After approval confirmation, update/read the CLOB allowance cache with
`CONDITIONAL-V2` for V2 positions and `CONDITIONAL` for CTF.

No production keys were used for verification. Public CLOB/Data API v2 reads
were blocked by the access environment; local HTTP fixtures and independent
signing vectors verify the wire contract, not a funded end-to-end deployment.
Existing EOA/Safe redemption polling is retained; uncertain pending chain
transactions require reconciliation before an operator retries them.

## Sources

- [Data API v1 to v2](https://docs.polymarket.com/migrate/data-api-v1-to-v2)
- [Protocol V2 API integration](https://docs.polymarket.com/migrate/polymarket-v2/api-integrations)
- [Protocol V2 contract integration](https://docs.polymarket.com/migrate/polymarket-v2/contract-integrations)
- [Contract addresses](https://docs.polymarket.com/resources/contracts)
- [Heartbeat](https://docs.polymarket.com/api-reference/trade/send-heartbeat)
- [Matching engine](https://docs.polymarket.com/trading/matching-engine)

Independent test vectors use viem 2.38.6 with the documented EIP-712 domain and
order structure; they use the public Hardhat test key, never an account key.

## Local verification

- Polymarket regression suite: 581 passed, 40 explicitly ignored. Covers
  private replay, duplication, bounded overflow, reconnect and instance isolation.
- Instrumented HTTP tests: 19 passed, 4 explicitly ignored.
- Focused follow-ups cover final cursor/archive filters, condition width aliases
  and immutable signing selectors across event rotation for all four wallet types.
- `python3 scripts/benchmark_polymarket_protocol.py` compiles the production
  cache/selector/digest functions with `rustc -O` against release sha3. Each arm
  has 200,000 samples; preparation, allocation, ECDSA and transport are excluded.

| Arm | Median ns | P99 ns | P999 ns | Maximum ns |
| --- | ---: | ---: | ---: | ---: |
| Previous fixed CTF domain + digest | 533 | 548 | 717 | 17,923 |
| Prepared mixed V1/V2 domain + digest | 536 | 554 | 718 | 3,116 |

Boundary: selector entry through the returned EIP-712 digest. Queue depth and
overflow: N/A (synchronous CPU stage, no queue). Source hash:
`6f6db29294b91473e197c923fd3b1f2309bac29f32ea0bdebf2f71fd54455941`.
Tail maxima include host scheduling noise. These measurements do not establish
production P99/P999 across dispatch, exchange acknowledgements or private fills;
that requires the existing live lane telemetry on the deployment host.
