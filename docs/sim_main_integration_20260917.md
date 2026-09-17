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

## Release performance check

Local optimized build, default release profile (fat LTO, one codegen unit). Each selection/lookup case contains 100,000 samples; times are ns. The selection boundary includes inference and bounded audit enqueue, excludes drain. The lookup boundary is one clean-level query, with ten pending price levels in the enabled case.

| Case | Median | P99 | P999 | Max | High-water / capacity | Overflow |
|---|---:|---:|---:|---:|---:|---:|
| Selection/shared | 120 | 126 | 129 | 21,200 | 128 / 16,384 | 0 |
| Selection/roles | 120 | 126 | 129 | 23,457 | 128 / 16,384 | 0 |
| Selection/channels | 125 | 131 | 158 | 21,413 | 128 / 16,384 | 0 |
| Match-time lookup/off | 65 | 69 | 71 | 19,681 | 0 / 4,096 | 0 |
| Match-time lookup/on | 109 | 119 | 168 | 29,904 | 10 / 4,096 | 0 |

The paired ledger benchmark alternates off/on ordering, with a fresh core per sample and 1,000 warmups per arm. Each case below contains 20,000 samples. Setup, cache warmup, assertions, destruction and reporting are excluded.

| Scenario / ledger | Median | P99 | P999 | Max |
|---|---:|---:|---:|---:|
| same_snapshot_two_takers / False | 3439 | 3739 | 23559 | 27763 |
| same_snapshot_two_takers / True | 3787 | 4079 | 24902 | 74881 |
| one_print_two_fifo_makers / False | 4076 | 4241 | 24274 | 74246 |
| one_print_two_fifo_makers / True | 4111 | 4261 | 24097 | 32877 |

The taker boundary is two sequential submissions on one snapshot; the enabled ledger caps combined execution at the available 10 shares (off: 12), so final workloads differ. The maker boundary is one public print applied to two same-level FIFO orders; both arms execute seven shares. Order evidence high-water is two; pending prints and message queues remain zero; all overflow counters are zero. Enabled print/FIFO capacities are 4,096/1,024.

All three benchmark tests passed. These measurements describe local simulator CPU cost, including scheduler noise. They do not measure live receipt-to-dispatch P99/P999 or claim production latency improvement. Raw integration logs are retained under `hexbot/results/integration/pm2_forward_f14_20260917/`.
