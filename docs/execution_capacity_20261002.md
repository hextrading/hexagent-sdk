# Execution capacity isolation and owner recovery quotes

A slow or repaired physical lane previously halved the whole degraded placement pool. With four ready lanes and two requests in flight, two healthy idle lanes could remain unavailable until a slow sibling finished. Degraded admission now admits each verified free lane, plus at most one unverified/prewarmed lane in flight. Account-wide Paused/Recovering, unknown POST ownership, generation fences, cumulative failures, leases, and ordinary cancel requirements remain unchanged.

`Strategy::take_execution_requote` transfers a single owner-local recovery edge to the existing live worker. Admission callbacks only store state. Recovery quoting runs after queued private/control/lifecycle/history messages and market snapshots are consumed, uses current processing time and ordinary strategy safety checks, and advances outer quote cadence. A fresh normal quote consumes the same pending edge without an additional idle callback. Repeated snapshots coalesce; shutdown/startup never consume an edge. Idle recovery orders carry `execution_capacity_resume`; publication-to-apply, publication-to-requote and callback duration are separate prepared latency stages so fresh-order timestamps do not hide control delay.

No new thread, lock, growing queue, shared risk authority, or mutable strategy/account reader was added. Connection observations and admission remain capacity-one full snapshots with cumulative adverse evidence; order/private lanes remain lossless and owner-routed. New counters, masks and blocked-time fields are solely dispatcher-owned; `place_blocked_total_ms` includes zero-capacity Degraded/Recovering. Compact diagnostic copies use the existing bounded background lane. More than 64 physical lanes retain full counts but masks display the first 64.

Validation covers partial repair with verified idle capacity, only one probation request, a slow completion without sibling suppression, duplicate observations, account isolation, and owner recovery deferral until private processing completes. Existing reconnect, no-response, generation, cancellation and overflow regressions remain in the engine/exchange suites.

Focused local benchmark: actual baseline `88ec064` and modified admission modules were compiled with `rustc -O`, immutable DTO definitions supplied by a standalone harness. Boundary is one full `observe` plus final `lane_place_allowed`, excluding HTTP, transport queues, diagnostics and OS placement. Each row has 200,000 events; the direct harness has queue depth/overflow 0. This is a macOS control-path measurement, not Linux end-to-end performance.

| State/source | median ns | P99 ns | P999 ns | max ns | free permits |
|---|---:|---:|---:|---:|---:|
| Healthy baseline | 221 | 470 | 522 | 63,381 | 4 |
| Healthy modified | 252 | 461 | 543 | 16,373 | 4 |
| Repaired probation + verified request, baseline | 205 | 458 | 524 | 27,028 | 0 |
| Repaired probation + verified request, modified | 237 | 511 | 552 | 18,814 | 2 |

The same prepared partial-pool scenario rejected all 200,000 spare-lane checks before and allowed them after. Recovery quotes still depend on fresh strategy inputs; truly unavailable capacity remains fail-closed. Live before/after quotation intervals, recovery delay, HTTP, private application and queue watermarks must be measured independently after rollout.
