# Historical complementary-token replay proof

The maker02 BTC02 incident started at 2026-09-23 15:25:08 UTC: gap replay
revisited a previously booked and confirmed taker trade after its market had
settled and the wallet's ten-minute token-interest grace had expired. The
private validator could no longer prove the complementary tokens, retained a
private-event anomaly, and stopped quoting and split admission.

The fix extends the existing immutable account pair publication with exact
two-token `settled_audit_references`. Those records already persist in the WAL,
survive wallet-interest expiry, and remain after FIFO release until all instance
lifecycle owners certify safe GC. They reconstruct the proof on startup without
a schema migration or a separate history store. Wallet-interest pruning, audit
retention and final GC refresh the publication on their existing cold paths.
Conflicting/non-binary bindings deny proof. The private reducer, identity and
frozen economics checks remain unchanged.

The original regression failed on the pre-fix code after wallet-interest pruning
and registering the next event. It passes with the fix and checks proof retention
through two owners' releases followed by removal at certified GC. Additional
tests cover WAL/restart reconstruction without reviving wallet query scope,
foreign account isolation, conflicting pairs, non-binary audit records, cold
revalidation of an existing anomaly without rebooking, startup cache reseeding,
and rejection of changed condition/token/side/quantity/notional. Existing lane
overflow, stale generation, ordering and idempotence regressions also pass.

Validation (local macOS, development profile):

- `cargo test -p hexagent-account --lib`: 331 passed, 15 ignored.
- `cargo test -p hexagent-exchange -p hexagent-engine -p hexagent-runtime --lib`:
  exchange 919 passed / 39 ignored; engine 137 passed / 7 ignored. Runtime had
  one failure in the existing global HTTP-timeout setting assertion under the
  parallel harness (75 passed / 1 failed / 2 ignored).
- `cargo test -p hexagent-runtime --lib -- --test-threads=1`: 76 passed, 2 ignored.
- `cargo check --workspace --all-targets`: passed. Existing warnings remain.
- `git diff --check`: passed.

Focused cold-publication benchmark: two current wallet interests and five
retained audits, 100,000 events each. Before uses the exact old publication
helper; after includes the five additional historical proofs. Boundary is
prebuilt cold state to fully constructed map; excludes ArcSwap publication,
destruction, I/O, queues, private dispatch and strategy processing. It ran while
other local validation was active, so tails include host contention.

| Helper | N | Median ns | P99 ns | P999 ns | Max ns | Queue depth / overflow |
|---|---:|---:|---:|---:|---:|---:|
| Before | 100,000 | 1,344 | 1,410 | 2,112 | 24,845 | 0 / 0 (no queue in boundary) |
| After | 100,000 | 5,590 | 20,716 | 29,374 | 113,589 | 0 / 0 (no queue in boundary) |

The increased cold construction cost buys longer-lived proof; this is not an
end-to-end improvement claim. No measured production P99/P999 is available for
this fix. No new worker, affinity role, channel, queue policy, quote-path lock,
allocation or account read is introduced. Existing cold state owns the audit
records; private owners consume only its immutable publication. The map's scope
is current interests plus references already retained by the existing GC, not
all past markets. Market routing and exclusive execution ownership are unchanged.
The existing cold control locking remains migration debt; this change does not
add it to a critical lane.

Reproduce the focused measurement with:

```sh
cargo test -p hexagent-account benchmark_historical_execution_pair_publication -- --ignored --nocapture
```

This change is source-level validation only. It neither clears production risk
state manually nor deploys/restarts maker02.
