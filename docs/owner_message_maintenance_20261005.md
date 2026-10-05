# Account-owner messages for maintenance progress

A strategy watchdog previously read the settled writer's Mutex and cloned its
full recovery checkpoint under that lock. Account snapshot polling also acquired
a process-global Mutex<HashMap> to choose a poller. Both operations occupied the
same strategy thread that receives market and private lifecycle events.

The account's existing cold owner now exclusively owns its snapshot lease.
Claims and generation-fenced finish messages use its existing bounded 4,096
command lane. Replies use a capacity-one polling lane. Instance/account identity
and a monotonically increasing request generation prevent a stale/duplicate
finish from releasing or renewing another request. Lease expiry recovers lost
callers; this coordination is never an order-admission or economic risk gate.
No new worker or CPU placement is introduced. Account metadata stays in the
cold owner, while private lifecycle work retains its higher-priority lane.

`try_submit_sidecar_checkpoint` returns the exact payload on full/disconnected
admission so callers can retain and retry it without cloning. The previous API
remains available. The dispatcher pre-registers `strategy.quote_signal_to_executor`
before processing traffic, removing first-use stage registry allocation/locking.

Validation: `cargo test --workspace --lib --locked`: 1,722 passed, 104 ignored.
Tests exercise expiry/takeover, account and instance isolation, duplicate/stale
finishes, abandoned replies, bounded overflow and retry, and exact sidecar payload
ownership on admission failure. A lease command is executed while the aggregate
account mutex is held to prove the new owner-local state does not borrow it.
A focused release benchmark records enqueue and owner-reply boundaries, count,
median, P99, P999, maximum, depth and overflow; it excludes network and actual
cross-core scheduling and is not an end-to-end latency claim.

Existing cold account operations still have their own locks and compatibility
APIs. This change removes lease shared state rather than extending that design;
caller migrations must use typed asynchronous completion, not synchronous waits
from a strategy thread.

Local x86_64 macOS release (LTO off, 16 codegen units), 1,000 warmups:

```text
test account::shared_account::snapshot_lease::tests::snapshot_lease_message_benchmark ... snapshot_lease_message stage=claim_enqueue n=10000 median_ns=237 p99_ns=296 p999_ns=331 max_ns=14144 queue_high_water=1 overflow=0 fixture=single_thread_owner_dispatch excludes=network_and_cross_core_scheduling
snapshot_lease_message stage=claim_to_reply n=10000 median_ns=456 p99_ns=518 p999_ns=563 max_ns=17073 queue_high_water=1 overflow=0 fixture=single_thread_owner_dispatch excludes=network_and_cross_core_scheduling
```
