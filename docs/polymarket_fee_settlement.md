# Versioned Polymarket fee settlement

A V2 taker BUY pays its fee in collateral in addition to its trade notional and receives the full outcome quantity. SELL fees remain deducted from collateral proceeds. The official [V2 Trading.sol](https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/Trading.sol) implements this in `_settleTakerOrder`, `_prepareMakerOrder`, and `_distributeBuyMakerProceeds`. The existing curve calculation is unchanged; `usdc_fee` remains the compatible ledger field name for collateral fees.

For example, 14 BUY shares at 0.85, rate 0.07 and exponent 1 have a collateral fee of 0.12495. V2 books cash −12.02495 and shares +14; the historical V1 convention books cash −11.9 and shares +13.853. The convention must not be inferred from fee amount zero, side alone, or the latest market metadata.

## New executions and historical records

`FeeSettlement::{LegacyV1, CollateralV2}` is explicit on `BinaryOption`, `TradeRecord`, and `RestoredTrade`. Old instrument/PM JSON fields default to LegacyV1. This preserves historical accounting provenance, including old records produced by the previous incorrect V2 model; it is not a claim that those executions used the V1 contract.

The live V2 adapter supplies its protocol and an explicit `FeeBasis` fallback on both maker and taker application. WebSocket, REST trade recovery and gap replay use the same private parser. For a new trade, the existing token registry supplies rate/exponent (including a prior event's LegacyV1-tagged curve), while execution V2 fixes the settlement asset. Without a registry, the current BTC crypto adapter explicitly supplies rate 0.07 and exponent 1, matching the configured app default. This is an adapter policy, not an account-layer inferred zero fee; non-crypto adapters need an appropriate explicit policy. Metadata absence does not add a maker/taker quote gate. Already-known trade IDs retain their frozen convention and fees, including historical zero-rate and FAILED trades.

The account-owner result includes `Option<TradeFee>` and the authenticated private `OrderUpdate` carries it unchanged through the existing lossless lifecycle lane. `Some` certifies chosen accounting amounts, including a legitimate zero, and FAILED retains the original amounts for reversal. Strategies use this message instead of independently recalculating against a possibly newer instrument curve. This closes the previous-event/default versus concurrent metadata-update race without a shared account read. Old messages deserialize with `None` and retain their compatibility path.

`register_token_fee_config_with_settlement` updates future executions. Before replacement, the existing lifecycle owner transaction freezes prior per-trade curves against the previous registry. Both registration APIs are prospective: neither reprices historical trades nor restores shares from an already-redeemed event. Frozen curves include zero-rate, maker-zero and FAILED rows. An old unattributed row with no compatible curve remains pending; the implementation does not guess an historical curve or silently migrate its accounting.

Snapshot validation and replay use each trade's frozen curve. FAILED reverses its original booked amounts; terminal and duplicate replays cannot rebook them. Existing wallet residuals require separate evidence and are not bulk-adjusted by this change.

## Strategy accounting and reservations

All PositionManager BUY cash calculations subtract collateral fees; share calculations continue subtracting only an explicit share fee. Versioned PM snapshots remain schema version 1 with compatible defaults. `fee_attributed` defaults to true for old PM rows, protecting genuinely zero-fee history. Restored unresolved rows explicitly carry false.

`attribute_restored_trade_fee` permits one fee-only attribution on one existing unresolved row, after validating identity, role and fee currency. It updates that row and existing incremental aggregates without replaying principal, volume or reservations. Already-attributed repricing fails. The strategy owner then applies the durable lifecycle status normally. For unresolved MATCHED followed by authoritative FAILED, fee attribution and ordinary reversal occur in the same owner callback and together reverse only the previously booked gross amount.

BUY reservations remain conservatively aligned with the existing SDK policy: each remaining share reserves `price * (1 + fee_rate_bps / 10000)`, including maker-only orders. Actual fees still use the curve. `PendingOrder.cash_fee_per_share` preserves this component during partial fills and FAILED reversals. `OrderOwnership.reservation_cash_per_share()` respects explicit new values and the bps fallback for historical records. Unknown order outcomes do not release their reservations.

## Ownership and validation boundaries

The strategy thread owns PM records and aggregates. Protocol, explicit fallback and attribution fields are Copy scalar message/snapshot data; the new optional fee payload increases each existing lifecycle envelope by a fixed amount. Metadata changes use the existing bounded account-lifecycle mailbox, retaining its backpressure/error behavior, before its owner freezes provenance and persists asynchronously. No new queue, worker, quote-path lock, blocking IO or growing quote container is added. Legacy cold account transactions retain their existing aggregate ownership/mirroring machinery; no strategy reads a mutable account for admission.

Functional checks cover actual private parsing, V2 BUY/SELL asset selection, duplicates and terminal ordering, exact FAILED reversal, restart/WAL, explicit fallback without a new quote gate, selected-fee message/serde compatibility, invalid fallback rejection, old JSON defaults, curve/version changes, zero fees, redeemed-token non-resurrection, instance isolation, fee-only attribution and conservative partial-fill reservations. The scalar benchmark measures curve calculation plus fee-asset selection only; its N/P50/P99/P999/max are not network or end-to-end production latency, and it has no queue. Existing queue/owner tests retain the transport backpressure checks.

The manual `wallet trades` report still uses its historical fee-estimation convention; it is not the live ledger authority and should not be used to recompute mixed-version history. A future reporting migration must use per-execution protocol/provenance rather than current market metadata.

Compatibility is forward-reading only: the updated reader accepts old ledgers, while an older binary's currency validator rejects new V2 BUY collateral-fee rows. A rollback after new fills must retain the compatible reader or use a separately reviewed ledger recovery procedure; discarding new ledger history is not a valid rollback.

Focused validation is recorded in the companion evidence logs and JSON. The scalar benchmark measures curve-plus-asset-split only; it is an unpinned, unoptimized local macOS measurement, not production before/after latency or a claim that HTTP tails improved. Existing private-lane focused tests exercise routing, FIFO/backpressure and owner identity with the expanded message.
