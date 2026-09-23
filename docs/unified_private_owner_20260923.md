# Three private account owners on 16 vCPUs

`os_tune.private_owner_cores` selects one lifecycle event loop per physical
account. It owns private ingress routing/deduplication, execution lifecycle,
`LivePositionManager`, replay state and recovery enrollment/acknowledgement.
Strategy threads still own their independent `StrategyAccount` and strategy
state. No account is shared between the three BTC strategy instances.

## Ownership and placement

The lifecycle thread binds the existing account lifecycle owner and takes the
recovery state and its sole receiver from the temporary startup recovery thread.
That temporary thread exits. Installing ingress transfers all routing state
before the websocket producer starts; no second private routing thread starts.
Recovery requests from the bound owner execute locally. External control/ACK
traffic uses the existing bounded mailbox. A thread-local binding identifies
exactly one account and is destroyed with that owner thread.

Each unified owner uses its configured CPU and `fifo_private_apply` priority.
Each account retains its own cold ledger thread using SCHED_OTHER.
`allow_shared_private_cold_core=true` permits those independent cold threads to
share a CPU; it never permits overlap with strategy, private owner, feed or HTTP
worker roles. Strict validation rejects mixed split/unified declarations,
missing cold CPUs, critical-role overlaps and active accounts without an owner
CPU. Configurations without `private_owner_cores` retain split ownership.

The intended application mapping is strategy CPUs 9/11/6, private account CPUs
10/12/13, cold CPU 5, dispatch/cancel CPU 14 and completion CPU 15. Existing I/O,
feed and router CPUs remain 2/3/7/8/4. CPUs 0–1 remain system/IRQ CPUs.

## Message and backpressure behavior

| Lane | Capacity | Owner / behavior |
| --- | ---: | --- |
| Live ingress | 1,024 frames | WS to account owner, FIFO; retain a frame iterator and process one record per turn; a fence follows the entire frame |
| REST replay | 1,024 batches | Recovery producer to the same owner; one record per turn with completion and recovery certificate checks |
| Private delivery outbox | 1,024 envelopes | Preallocated, owner-local FIFO; retain exact destination and update on downstream fullness |
| Commit acknowledgements | 1,024 batches | Existing advisory deduplication lane; overflow cannot acknowledge an uncommitted event |
| Historical repair reply | 1 | Existing single in-flight repair credit; generation/certificate checks remain required |
| Recovery control / ACK | 16,384 commands | Same account owner in unified mode; external callers retain existing failure and timeout rules |
| Execution lifecycle / maintenance | 16,384 each | Existing lanes; service lifecycle every turn, maintenance at idle or every 64 active turns |
| Ingress installation | 1 | Startup only; duplicate or split-mode installation is rejected |
| GC plan | 1 plus one retained cold result | Cold coordinator scans immutable execution publications; only the lifecycle owner deletes its mutable state |

Every loop turn services recovery/output retry, account lifecycle control,
execution lifecycle, then private ingress. Live and replay each get at most one
record; maintenance/GC cannot starve. These are message-count bounds, not CPU
time guarantees for a large individual trade. Crossbeam transport on the
existing internal lanes is retained. The live engine's existing polling root
private sender is preserved.

When an output is retained, new ingress consumption pauses while lifecycle and
recovery control remain serviceable. A separate atomic backpressure gate pauses
new quotes without inventing a reconnect epoch. Draining the outbox clears only
that gate, never a reconnect proof or inventory-uncertain condition. Capacity
exhaustion/disconnection fails closed and requests authoritative recovery.
Stopping with undelivered envelopes retains the health gate and emits a shutdown
error; startup replay is required. No successful delivery is claimed for them.

Recovery fences cannot overtake earlier live frame records. Quote recovery still
requires strategy application acknowledgements. Duplicate and replay economics
continue using the existing frozen execution certificates and idempotent ledger.

## Cold work and remaining migration

Settled GC coordination and full execution-snapshot selection move to the cold
account thread. The private owner removes at most 16 execution identities and
16 indexed trade-history records per GC turn. It rechecks local token ownership
and atomically rechecks runtime client-order ownership before removing a route.
Readers retain immutable snapshots. Nonterminal history and rows added after
the retirement cursor survive.

No quote callback, global mutable map, strategy-account lock or synchronous
quote-path I/O is added. Existing account-local parsing/ledger code still
allocates and can use existing account control synchronization; this change
does not claim a fully allocation-free private lifecycle. Follow-up work should
profile those account-local stages and migrate them to preallocated records and
bounded owner publications, while preserving recovery correctness. Sharing cold
CPU 5 also requires live measurement of ledger/reconciliation queue pressure.

## Validation and measurements

Functional coverage includes large-frame ordering, duplicate replay, three
independent account lanes, full output retention with control progress, quote
pause/resume, input saturation, recovery fence/strategy ACK ordering, stale
epochs, recovery ownership handoff and bounded GC with rebound identities.

Commands:

```sh
cargo test --offline -p hexagent-exchange --lib -- --test-threads=2
cargo test --offline -p hexagent-runtime -p hexagent-config -p hexagent-account --lib
cargo check --offline --workspace --all-targets
cargo test --release --offline -p hexagent-exchange --lib benchmark_three_account_private_owners -- --ignored --nocapture --test-threads=1
```

The focused benchmark compares split and unified modes in the same current SDK,
not historical whole binaries. Each mode processes 12,288 events: three accounts
with 4,096 distinct trades each, in bursts of 32/account. Payload construction and
classification are outside the measured boundary. Start is immediately before
the classified frame enters the private lane. Endpoints are the existing
producer-ready timestamp, receipt by the benchmark consumer, and completion of
local lifecycle application. Split application completes a whole 32-record
batch; unified application completes each record. Consumer measurements include
the harness's sequential per-account draining and scheduling delays.

The host is macOS x86_64, Intel i9-9880H, 16 logical CPUs. Pinning and FIFO are
explicitly disabled. Results exclude network, JSON classification, strategy
application, quote generation, HTTP ACK, disk flush and production CPU topology.
They do not establish production end-to-end P99/P999 or prove the chosen
16-vCPU plan is globally optimal. All distributions, queue sampling boundaries
and overflows are retained in the adjacent benchmark evidence. In particular,
combining routing with lifecycle work can increase routing latency during bursts
even when lifecycle median improves; this tradeoff must remain visible.
