# Archive float round-trip repair — 2026-09-29

A retired maker02 taker proof was archived with normalized price
`0.040000000000000036`, then rehydrated as `0.04000000000000003` by the default
serde_json 1.0.150 float parser. The next retention pass rejected the immutable
payload every five seconds. The incident recorded 1,625 warnings for this key.

Enable `float_roundtrip` for account JSON, including archive, checkpoint and WAL.
Cargo unifies this feature for other consumers of the same serde_json version.
For already persisted legacy prices, only adjacent-ULP taker prices with identical
identity, quantity, status, flags, raw execution price and gross notional may use
the original archived proof. The archive price times quantity must also exactly
match its independently saved gross notional. Validate the existing checksum
before accepting duplicates; never overwrite original payloads. Hydration can
replace the legacy hot copy with that authoritative proof. All other conflicts
remain fail-closed. This does not adjust balances or replay economic mutations.

`cargo test -p hexagent-account -p hexagent-exchange` passed: 362 account and
955 exchange unit tests, plus default integration/doc targets. Existing ignored
live-network/fixture/benchmark cases remain ignored. New regressions cover
repeated round trips, both legacy checkpoint recovery paths, WAL restart,
duplicate/out-of-order terminal events without rebooking, identity/economic
conflicts, checksum corruption and batch rollback. Existing tests cover bounded
lane overflow, private-event ordering/reconnect and instance isolation.

No mutable business fields, messages, queues or threads are added. SQLite reads,
checksum checks and compatibility clones stay on the existing cold owner. Quote
processing and instance ownership are unchanged. CLOB uses its resident simd-json
parser; numeric RTDS and other serde_json consumers inherit the feature. Existing
cold-owner/account locking is not expanded into critical lanes; future ownership
migration remains separate. No live deployment or end-to-end latency measurement
was performed, so the following does not establish live P99/P999 or queue health.

## Focused parser measurement

The standalone crate is deliberately outside the SDK workspace so the default
parser baseline does not inherit feature unification. Reproduce with:

```sh
cargo run --release --manifest-path tools/archive-json-float-bench/Cargo.toml
cargo run --release --manifest-path tools/archive-json-float-bench/Cargo.toml --features precise
```

Intel i9-9880H, macOS x86_64, release build; 10,000 warmups followed by 100,000
individual decode/drop timings per case, three alternating baseline/fixed rounds.
Numbers are nanoseconds. The RTDS case is a borrowed typed numeric wire-shaped
payload; the cold case decodes the captured archive proof into `Value`. Atomic
allocation instrumentation is present in both builds. Queues are not part of
this synchronous harness (depth/overflow 0); network, strategy, dispatch, SQLite
and downstream queues are excluded. Scheduling tails on this unpinned workstation
are visible. Preliminary runs during compilation were discarded.

```text
round=1
repro bits=3fa47ae147ae147f expected=3fa47ae147ae1480 exact=false
mode=default case=borrowed_rtds_numeric n=100000 p50_ns=330 p99_ns=765 p999_ns=852 max_ns=42928 allocations=0 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
mode=default case=actual_archive_proof_value n=100000 p50_ns=4126 p99_ns=8268 p999_ns=9953 max_ns=93695 allocations=2900000 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
round=1
repro bits=3fa47ae147ae1480 expected=3fa47ae147ae1480 exact=true
mode=precise case=borrowed_rtds_numeric n=100000 p50_ns=340 p99_ns=408 p999_ns=605 max_ns=41107 allocations=0 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
mode=precise case=actual_archive_proof_value n=100000 p50_ns=4490 p99_ns=8582 p999_ns=14154 max_ns=710613 allocations=2900000 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
round=2
repro bits=3fa47ae147ae1480 expected=3fa47ae147ae1480 exact=true
mode=precise case=borrowed_rtds_numeric n=100000 p50_ns=352 p99_ns=492 p999_ns=774 max_ns=26614 allocations=0 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
mode=precise case=actual_archive_proof_value n=100000 p50_ns=4553 p99_ns=7167 p999_ns=9794 max_ns=98738 allocations=2900000 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
round=2
repro bits=3fa47ae147ae147f expected=3fa47ae147ae1480 exact=false
mode=default case=borrowed_rtds_numeric n=100000 p50_ns=340 p99_ns=409 p999_ns=740 max_ns=26825 allocations=0 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
mode=default case=actual_archive_proof_value n=100000 p50_ns=4237 p99_ns=8618 p999_ns=24763 max_ns=734121 allocations=2900000 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
round=3
repro bits=3fa47ae147ae147f expected=3fa47ae147ae1480 exact=false
mode=default case=borrowed_rtds_numeric n=100000 p50_ns=341 p99_ns=750 p999_ns=840 max_ns=113938 allocations=0 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
mode=default case=actual_archive_proof_value n=100000 p50_ns=4239 p99_ns=8544 p999_ns=20991 max_ns=109158 allocations=2900000 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
round=3
repro bits=3fa47ae147ae1480 expected=3fa47ae147ae1480 exact=true
mode=precise case=borrowed_rtds_numeric n=100000 p50_ns=364 p99_ns=762 p999_ns=846 max_ns=23844 allocations=0 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
mode=precise case=actual_archive_proof_value n=100000 p50_ns=4792 p99_ns=9204 p999_ns=14279 max_ns=150817 allocations=2900000 queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch
```
