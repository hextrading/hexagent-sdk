# Fast ownership publication and dispatch identity tail

Baseline: SDK `6a40425dba6899ec6273238fbfcb110b2849cc1b`; the maker02 run used SDK `94aa2871` (the intervening changes concern replay policy/artifacts). The prior 70-minute live audit measured a 143.527 us Fast owner queue maximum. Two preceding preparations on CPU 14 overlap 134.452 us of that wait. CPU-aware dispatch already exists: availability also depends on each account's healthy, unoccupied HTTP slots, so this observation alone does not prove a bad CPU choice.

## Changes

* Skip write-side RCU for unrelated ownership hash collisions. The previous no-op update still paid ArcSwap reader debts. The actual publication rechecks identity under RCU, remains visible before HTTP bytes, and still rolls back on failed bounded registration handoff.
* Borrow the startup-bound numeric owner's instance string at root dispatch instead of cloning both the embedded and bound strings. Cold/system compatibility still owns its identity; mismatch validation is retained.
* Append `identity_probes` and optional `fast_selection` to the existing trace. The latter records selected core, eligible/busy/unhealthy candidate counts and minimum/selected pending preparation work before enqueue. Zero probes or absent selection means unknown. Candidate counts partition the ranking snapshot (busy takes precedence over unhealthy); they are observations, never admission permissions. Pending work can include running preparation and excludes HTTP reply wait. A candidate can become free between ranking and enqueue; minimum is null when no candidate was eligible at ranking.

No new worker or queue. The dispatcher is the sole writer of selection fields; a Fast owner writes probe count. Existing owned commands transfer the trace; existing background audit serializes it. Account state, reservations, generation gates, private event losslessness and CPU placement are unchanged.

## Focused benchmark

macOS x86_64 release, LTO disabled, 16 codegen units. Each cell has 10,000 measured publishes after 1,000 warmups; 32 registered reader threads; depth is synthetic same-bucket occupancy. Inputs are prepared before timing, removal is after timing. Boundary: prepared ownership -> identity publication. Queue depth and overflow are both zero. Nanoseconds; columns are median / P99 / P999 / max.

| Collisions | Previous | Changed |
|---:|---|---|
| 0 | 1745 / 1789 / 15536 / 21880 | 1760 / 1846 / 1903 / 21639 |
| 8 | 14567 / 17823 / 51965 / 62674 | 1922 / 1963 / 1983 / 21161 |
| 32 | 53230 / 100722 / 163388 / 300698 | 2390 / 2440 / 2481 / 21830 |
| 64 | 104696 / 602329 / 10786542 / 10895692 | 3281 / 11712 / 28930 / 38387 |

The zero-collision median/P99 are slightly higher due to the extra read. Synthetic collision depth is not the live distribution. Host scheduling is included in maxima. This is not evidence that the entire live 143.527 us tail is eliminated; probe counts and CPU selection now allow live attribution.

## Correctness and migration boundary

Collision tests cover concurrent writers, sibling isolation, holes, bounded probe exhaustion and replay. Existing tests exercise private identity before send, registration overflow/disconnect rollback, numeric ownership, generation-aware admission, cumulative completion replacement and busy-slot exclusion. Historical MessagePack traces decode appended fields as unknown. Tests invoking the real strategy worker now use its production 8 MiB stack budget; default test threads were too small with the extended inline trace.

Remaining costs: the actual ArcSwap publication still coordinates readers and allocates an immutable Arc; order/signature strings and downstream owned command identities still allocate. Removing those needs preallocated generation-tagged identity slots with safe lifetime/reuse, while preserving the pre-send private-event routing guarantee. The existing per-core preparation scheduler uses dispatcher-local Rc/RefCell (no cross-thread lock); a single immutable completion counter per owner crosses threads. Signatures already use a startup-owned libsecp256k1 context.

The latency registry's Mutex is used at thread/stage initialization; unregistered `record_ns` labels can still take it, so new hot stages must be prepared. Supervisor epoch/readiness locks remain on feed control/rebuild paths; they are not proof of a per-message quote lock. Legacy histogram binning remains producer-side on some `record_ns` paths and should incrementally move to prepared bounded observation lanes. This change adds no histogram work.

Validation: `cargo test --workspace --locked` completed successfully, including engine routing, exchange/account, runtime, historical trace decoding, integrations and doc tests. Live network integration cases remain explicitly ignored. The ignored focused benchmark above was run separately.
