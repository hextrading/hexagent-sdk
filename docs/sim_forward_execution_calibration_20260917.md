# Forward F12: execution validity, liquidity conservation and channel calibration

## Scope

The experiment starts from the frozen forward F12 implementation. The original
F12 executable and configurations remain controls. The corrected profiles use
the existing `staged_rtt_fraction` protocol with legacy finality disabled. The
20% processing fraction is an explicit modeling assumption inside observed RTT,
not a measured exchange processing time. Existing private-fill scheduling and
the single-writer simulation core remain responsible for all economic events.

The four stages are cumulative: cancel validity, quantity ledger and own-order
FIFO, match-time liquidity, and channel probability/quantity calibration.

## Match-time liquidity

The new core mode shares `sim_v2_match_time_liquidity` with the simulator and
requires the canonical quantity ledger. It observes same-direction public
prints at each price after the latest accepted source-clock book. A taker can
spend only the displayed quantity minus simulated consumption and the still
unreflected public quantity at that exact price. An old 950ms minimum-depth or
1500ms trade window is not also deducted.

Since these archives lack reliable exchange trade timestamps, the interval
uses trade application order and accepted book source-clock evidence. A delayed
public print can already be reflected in a received book; without a shared
sequence number this remaining ambiguity cannot be identified exactly.

An accepted snapshot supersedes this interval's public evidence; ignored older
or sibling snapshots do not clear it. Prints at/before the latest source clock
are excluded from this interval. This uses reconstructed message ordering and
is not a claim to know the exchange's full order-level queue.

### Public decreases must not erase simulated consumption twice

Example: the book contains 10 shares, a public trade removes 6, and our simulated
taker consumes the remaining 4. A later snapshot showing 4 explains the public
trade; it does not provide another 4 shares to the simulator. Before the old
minimum-replacement debt reconciliation runs, the known public decrease is
prevented from also reducing hypothetical own consumption. A later snapshot
of 10 restores only the newly observed 6 shares.

The added regression test exercises this sequence and verifies that unrelated
snapshots do not release evidence, available trade IDs deduplicate prints, and
evidence overflow aborts rather than grows the buffer or invents liquidity.
Archives without exchange trade IDs still cannot prove cross-record deduplication.
The existing `liquidity_ledger.reconciled_qty` counter includes debt cleared after
this public-decrease compensation; it is not a separate measurement of own
executed quantity and must not be compared to `consumed_qty` alone as an
accounting identity. Actual fill limits and available depth are verified by
regression tests and execution audits.

## Selection model schema 2

Schema 1 remains compatible. Schema 2 adds optional `maker_trade`, `maker_book`
and `taker_sweep` models. Each contains a `fill` model with the existing features
and optional conditional `quantity` coefficients. Both links are logistic. The
existing role strength blends retention and quantity with the identity mode:

```
retention = 1 - strength * (1 - model_retention)
quantity_fraction = 1 - strength * (1 - model_quantity)
```

The stable owner/order/role rank continues to determine retention; repeated
candidates do not get a new random draw. Quantity remains between zero and the
already eligible candidate. Missing channel models inherit the role model;
missing quantity models retain full candidate quantity. Zero strength and
collect mode preserve candidate quantity. Forward information is consulted
only in the explicitly selected forward mode.

A partial taker selection rescans the actual limit-constrained price ladder to
compute the selected prefix's notional and fees. It never keeps a full sweep's
average price after reducing its size. FOK and wallet checks happen before
consumption is committed; a failed FOK spends no liquidity.

## Training evidence

Training uses corrected fixed-command candidates in the original train268
partition. Maker supervision is restricted to post-only orders with a unique
simulated fill channel. Reversed quantities and ambiguous channel/role samples
are excluded. The available labels are lifetime occurrence and aggregate
quantity ratios, not precisely identified fragment match times.

The downloaded private-trade audit lacks liquidity role and execution price.
Non-post-only Limit orders may later rest, so their lifetime quantity is not
used as a taker quantity label. Taker occurrence uses the observed initial
HTTP Filled proxy; taker quantity fitting remains disabled. The report must
include exclusions and must not describe these proxies as observed fragment
roles. Calibration selects strengths on tune89 only, then freezes the result
before seed43/44 evaluation. The historical test90 was previously inspected.

## Ownership, boundedness and performance

All new mutable fields belong to the existing simulator exchange thread. No
new strategy threads, live account readers, locks or cross-thread requests are
introduced. `unreflected_taker_prints` is allocated once at startup with 4096
slots, aggregates existing price/side entries in place, clears on snapshots or
event retirement, and fails on overflow. Its high-water mark is exported in
the existing offline summary. Model inference uses fixed nine-element arrays;
new branches perform no file/network I/O or training.

The existing offline matcher still has older map/ladder allocations and audit
formatting. Those are not extended into the production quote path. Reusing any
of this inference in live quoting requires replacing those allocations with
preallocated buffers, draining compact audit events asynchronously, and
measuring owner-thread receipt-to-dispatch P99/P999 first.

Focused release measurements cover 100000 selection calls including bounded
audit enqueue (drain excluded), and 100000 clean-level availability queries
with ten pending price levels. Reports include median, P99, P999, maximum,
capacity and high-water/overflow. These are local microbenchmarks, not live
end-to-end latency claims.

## Artifacts

The frozen build, stage configurations, raw compressed audit logs, model fit,
selection, reports and validation reside alongside this document. New options
and models are research profiles; no global default or live deployment is
promoted based on the historical PnL fit alone.

## 最终验证

- 260 项 sim_v2 release 功能测试通过，0 失败；另执行两项忽略的性能基准并通过。
- 21 次完整策略回放、5 次固定命令回放通过独立验证。309 个 Rust 源文件与测试构建一致。
- 原 F12 seed42 的 447 event 经济字段逐字节复现；新配置均通过撤单时序、RTT、数量与审计排空检查，相关队列无溢出。
- [验证清单](/Users/Admin1/projects/hexbot/results/backtests/pm2_btc01_forward_mechanisms_20260917/VALIDATION.json) · [性能边界和 P99/P999](/Users/Admin1/projects/hexbot/results/backtests/pm2_btc01_forward_mechanisms_20260917/PERFORMANCE.md) · [本轮增量代码](/Users/Admin1/projects/hexbot/results/backtests/pm2_btc01_forward_mechanisms_20260917/incremental_sdk.patch)

## Economic acceptance

The selected maker=1.0/taker=0.5 profile reduced corrected-arrival net PnL+rebate
from 1452.26 to 920.45 USDC, but original forward F12 was 143.39 and live was
-231.61. Path RMSE increased from original F12's 164.95 to 578.06. The selected
model also leaves maker volume too low and taker volume too high. It is retained
as a research result and is not promoted as a better fitting default.

[Full comparison and missing-candidate evidence](/Users/Admin1/projects/hexbot/results/backtests/pm2_btc01_forward_mechanisms_20260917/REPORT.md).
