# Independent selection strengths and evidence-bounded maker recovery

This change extends the isolated V5/V6 simulator experiment. Production exchange
adapters and their quote threads are unchanged. All new options default to the
previous behavior; no fitted strength is promoted as a global default.

## Configuration

```toml
# Omitted overrides inherit the existing shared strength.
sim_v2_selection_strength = 0.25
sim_v2_selection_maker_strength = 0.5
sim_v2_selection_taker_strength = 0.0

# Experimental recovery; all three must be explicitly enabled.
sim_v2_liquidity_ledger = true
sim_v2_match_time_liquidity = true
sim_v2_maker_trade_through_recovery = true
```

The numeric strengths above illustrate the API, not a calibration recommendation.
`P_keep = 1 - strength_role * (1 - P_model)`. Zero bypasses learned suppression;
one applies the model's full suppression. No strength creates quantity or changes
an execution price. Both role overrides are validated atomically at startup. The
existing owner/order/role seeded rank remains stable across repeated candidates.
The audit includes the effective role strength.

## Missing candidates

The existing arrival-liquidity mode uses available depth at exchange matching,
including intervening replenishment, instead of a historical in-flight minimum
and old trade-volume haircut. Its quantity ledger debits simulated consumption
and requires observed replenishment before that depth can be spent again.

The new maker recovery mode accepts a strict trade-through witness: a public sell
below our resting bid, or public buy above our resting ask. A worse execution
price witnesses clearing of public queues at our better price. The hypothetical
own orders still receive at most that print's recorded shares in canonical
price/time order. Exact-price prints retain ordinary queue depletion.

This requires the existing canonical quantity ledger. It deduplicates available
venue trade IDs, shares one budget across owners and price levels, and subtracts
allocations before any subsequent trade-confirmed book-through. Missing venue
IDs retain the existing distinct-print assumption; this is not a claim that an
archive without exchange IDs can provide exact deduplication.
The frozen dataset uses legacy readers that construct `exchange_trade_id: None`.
Its runtime ID buffer is empty, so conservation in this experiment is per recorded
print, with no claim that separate archive rows have distinct venue execution IDs.

Recovery cannot restore rejected, cancelled, expired or not-yet-arrived orders.
Freshness/continuity, remaining-quantity and existing residual/toxicity constraints
still apply. The selector sees recovered candidates after these constraints. It
may reject them. The `trade_through` channel and total candidate/fill counters
make this source distinguishable. Future markout is used only when an explicitly
configured forward selector is active; candidate recovery itself uses the public
print already applied to the matching clock.

## Ownership and cost

All new fields belong to the existing single-writer simulator core. There are no
new workers, cross-thread messages, locks, live account readers, I/O or heap-growing
operations in the new branches. The existing ledger's stack-backed 1,024-order
price/time iterator and bounded 4,096-print evidence are reused. Its saturation
omits inferred fills and increments an overflow counter; experiment acceptance
requires zero overflow. Selection audit capacity remains 16,384 and overflow
aborts rather than silently dropping evidence. Existing simulator auditing still
has older allocations outside the new branches; this patch does not extend that
subsystem into the production quote path.
The incremental migration path for the pre-existing offline matcher allocations
is to preallocate and reuse fill batches and compact audit records, then perform
formatting/persistence in the replay drain. Any attempt to reuse this inference
path in live quoting must first complete that migration and measure end-to-end
P99/P999; this research patch does not authorize such reuse.

Focused release measurement: 100,000 `Selection::select` calls, including bounded
audit enqueue and excluding batched drain. Shared-strength mode P50/P99/P999/max
103/128/133/14,526ns; role overrides 88/108/111/22,178ns. Queue high-water 128,
overflow zero. This is a local microbenchmark under build load, not live end-to-end
latency evidence or a claim that the override is faster.

## Validation and result location

255 simulator unit tests passed; 13 opt-in benchmarks ignored in that suite.
New tests cover independent overrides and fallback, atomic invalid configuration,
stable rank, shared print budgets across owners, price priority, real limit price,
trade-ID deduplication, no second book-through spending, cancelled orders,
wrong-direction prints and opt-in compatibility. Existing tests cover bounded
queues, replay/reconnect, delayed private messages, stale books and isolation.

Full comparisons and the frozen tune-only calibration plan live at:
`/Users/Admin1/projects/hexbot/results/backtests/pm2_btc01_forward_role_recovery_20260916/`.
The forward teacher and legacy finality12 remain historical diagnostics. A better
PnL fit is not proof of observed-RTT physical validity. New candidate distributions
also require future independent calibration before promotion.
