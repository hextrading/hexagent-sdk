# Cancel-only pause and startup cash migration

Two issues found while preparing a UTC no-trade schedule and reactivating a
previously disabled account owner:

- Repeating `cancel_all_with` while POST ACKs were outstanding rebuilt the
  cancellation BTree and cloned IDs even when every order already had a cancel
  intent. The owner-local precheck now returns without mutation in that case;
  mixed walks also avoid cloning an already parked intent. Newly active or
  recovered orders still cancel normally. ACK ordering and overflow ownership
  remain unchanged.
- Engine startup attempted explicit virtual-cash migration before authoritative
  recovered-order reconciliation. A historical accepted order could therefore
  block the migration even when the subsequent recovery would release it. Move
  migration after the existing reconciliation, before workers can quote, and
  refuse Polymarket startup if an explicitly requested migration still fails.
  No reservation or ownership check is weakened. Migration remains durable and
  idempotent, and never moves historical token/trade/order ownership.

## Focused cancellation measurement

Same benchmark source and machine, before/after the OM change. Intel i9-9880H,
macOS, release/fat LTO; no CPU affinity. Each sample measures one complete
`cancel_all_with` call with 0/1/8/32 Submitted orders whose cancel intents were
already parked, including `Instant` timing overhead. Setup, sample storage and
sorting are outside the measured interval. The single-thread executable counts
heap allocations only inside samples. No network, queues or background workers
participate; queue depth and overflow are zero. This is not an end-to-end
latency claim, and maxima include host scheduling interference.

| Pending | Version | Events | Median ns | P99 ns | P999 ns | Max ns | Allocations |
|---:|---|---:|---:|---:|---:|---:|---:|
| 0 | Before | 100,000 | 39 | 49 | 52 | 1,754 | 0 |
| 0 | After | 100,000 | 33 | 34 | 35 | 78 | 0 |
| 1 | Before | 100,000 | 303 | 321 | 332 | 22,890 | 300,000 |
| 1 | After | 100,000 | 49 | 51 | 52 | 26,239 | 0 |
| 8 | Before | 100,000 | 1,375 | 1,443 | 15,443 | 30,328 | 1,000,000 |
| 8 | After | 100,000 | 396 | 403 | 433 | 27,578 | 0 |
| 32 | Before | 100,000 | 8,620 | 9,246 | 30,372 | 54,944 | 4,200,000 |
| 32 | After | 100,000 | 2,372 | 2,498 | 21,920 | 73,893 | 0 |

Reproduce the allocation-free steady-state assertion with:

```sh
cargo run --release -p hexagent-account --example cancel_pause_benchmark -- --require-no-alloc
cargo test -p hexagent-account --lib
cargo test -p hexagent-engine --lib
```

Account tests: 317 passed, 0 failed, 12 ignored. Coverage includes repeated
pause/ACK/duplicate ACK, recovered-order cancellation, instance isolation,
existing sink-overflow/tail restoration tests, and migration rejected with an
outstanding reservation then applied exactly once after terminal recovery,
preserving the previous instance's token holdings and order ownership.

Engine tests: 131 passed, 0 failed, 6 ignored.

No new mutable runtime field, lock, queue, worker, shared account admission,
or hot-path I/O was added. Account and OM state retain their existing sole
writers. Startup migration and its existing durable flush remain before the
strategy workers start.
