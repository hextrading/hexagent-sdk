# Live owner ledger queries

`PositionManager::enable_incremental_queries()` removes scans over historical
trades from `balance`, `get_quantity`, `available_cash`, and
`available_inventory`. The strategy owner enables it during live startup and
again after constructing or restoring a replacement ledger. The default stays
on canonical `BTreeMap` folds so backtest summation order does not change.

The trade ledger and version 1 snapshot remain authoritative and unchanged.
The optional index contains compensated cash sums and total/conservative share
sums per traded symbol; it is rebuilt from canonical records and is not
serialized. Accepted transitions update it using the stored economics, including
when replay fields differ within the existing tolerance. Rejected, duplicate,
out-of-order, and terminal replays leave it untouched. A FAILED reversal removes
the original contribution. A fee revision through the existing baseline rebuild
creates a fresh ledger; enabling queries then derives the corrected totals.

Unconfirmed BUY shares and SELL proceeds remain unavailable. Existing pending
order reservations are still folded separately over active orders, so cancel
timeouts, late accepts, failed-fill reservation restoration, and seed balance /
quantity adjustments keep their existing behavior. Reporting methods `get` and
`positions` continue to use the canonical ledger; moving their full snapshots
off strategy callbacks is separate work.

The same strategy thread owns the ledger and index. There are no new workers,
locks, queues, cross-thread messages, or queue/overflow semantics. Queries do not
allocate. First private activity for a new symbol creates one index entry;
subsequent updates do not clone its symbol. This adds O(traded symbols) retained
memory to the existing O(trades) ledger and does not add per-trade retained
records. Active pending lock folds remain O(active orders). Numeric results can
differ from default canonical folds by floating-point rounding; compensated sums
reduce error in long add/reverse streams. No risk gate or ownership rule changes.

## Validation

Focused oracle comparisons cover maker/taker BUY/SELL fees, Matched/Mined/
Confirmed/Failed ordering, first-seen terminal replay, duplicate and invariant
rejection, canonical economics on tolerated replay, conservative reservations,
cancel-timeout retention, late-accept resurrection, balance/quantity adjustment,
independent owners, v1 snapshot restore, baseline fee revision, and compensated
reversals. The oracle is the unchanged default ledger scan.

`cargo test -p hexagent-account`: 256 passed, 0 failed, 8 existing ignored
benchmarks. This includes five new focused index tests. The build reports the
existing `shared_account` dead-code / unused-result warnings.

## Reproducible benchmark

```sh
cargo run --release -p hexagent-account --example position_query_benchmark
```

The same release binary compares default canonical folds with the opt-in index.
Each case preloads historical trades across 256 symbols and 16 active order
reservations, warms up 100 private events, and times 10,000 further new Matched
trades. The history grows during each case; the CSV records both endpoints.
The measured PM application is `get_quantity` before mutation, admitted
`upsert_trade`, pending reservation consumption, `available_cash`, then
`available_inventory`. It includes the existing trade-ledger insertion and its
allocations. Input strings, setup, sample aggregation, logging, and I/O are
outside the measured interval. The surrounding StrategyAccount fields, exchange
transport, and private-event queue are not included. No queue exists in this
microbenchmark: queue depth and overflow are both 0, active orders stay 16.

Run on 2026-09-15 with rustc 1.97.1, x86_64 macOS, Intel i9-9880H 2.30 GHz,
workspace release profile (fat LTO, one codegen unit). The workstation is
unpinned and shared with other development work; absolute tail values include
host scheduling effects. Production end-to-end P99/P999 and queue depth must be
checked after deployment; this measurement only isolates the removed history
scan and does not establish a network-latency improvement.

All timing values below are microseconds. Each row has 10,000 measured events,
16 active reservations, queue depth 0, and overflow 0. Percentiles use the
nearest-rank definition on individual samples, not window percentiles.

| Retained trades during measurement | Query mode | Median | P99 | P999 | Maximum |
| --- | --- | ---: | ---: | ---: | ---: |
| 1,100–11,100 | Canonical scan | 120.828 | 284.299 | 317.181 | 1,763.543 |
| 1,100–11,100 | Incremental | 1.176 | 3.155 | 5.019 | 24.334 |
| 10,100–20,100 | Canonical scan | 261.767 | 472.151 | 504.258 | 751.064 |
| 10,100–20,100 | Incremental | 1.297 | 3.384 | 5.010 | 19.037 |
| 50,100–60,100 | Canonical scan | 1,103.417 | 1,985.708 | 15,802.096 | 29,702.509 |
| 50,100–60,100 | Incremental | 1.334 | 2.026 | 2.465 | 3.873 |
