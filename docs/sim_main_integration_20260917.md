# Replay integration on current main

The integrated simulator adds opt-in canonical quantity conservation, matching-time liquidity, maker/taker and channel selection, arrival-window diagnostics, historical market rules, and deterministic owner/recovery replay. Causal selection uses information already available at matching; forward selection remains an explicit offline diagnostic. These mechanisms do not change the existing default finality or queue-decay settings.

The integration preserves current main execution admission and recovery-progress handling. The simulator and virtual coordinator own the new mutable state. Each virtual owner has bounded market/private/history lanes, private-first ordering and explicit overflow failure. The strategy owns its recovery gate and oracle cache. No new production thread or cross-thread request is introduced. Existing allocation/persistence in offline replay stays outside production quoting; preallocation and batched drain are the documented migration path before any live reuse.

## Integration checks

- SDK workspace library tests: **1,422 passed, 54 ignored**, using `--test-threads=1`.
- Consumer hexbot workspace library tests with `sim-fidelity`: **1,255 passed, 35 ignored**, using the integrated SDK through a temporary Cargo path override.
- SDK workspace all-target compilation, including the command-replay example: passed.
- Consumer workspace all-target compilation with `sim-fidelity`: passed.
- Frozen F14 baseline/input tests: 9 passed; recovery/journal/experiment tests: 21 passed.

The first parallel SDK run exposed interference between the existing CLOB allocation tests: their allocation counters and tracked-thread selector are process-global. Both allocation tests passed in isolation and the entire SDK suite passed serially. No production parser change was needed. Ignored tests include explicit benchmarks and tests requiring external replay evidence.

The adopted hexbot baseline keeps the historical forward V5/F14 binary and `deep_queue_decay=0.005`. The new source is available for separately identified experiments; updating the SDK pin does not relabel historical economics as a new build. Prior fitting results and their limits remain in the accompanying experiment reports.
