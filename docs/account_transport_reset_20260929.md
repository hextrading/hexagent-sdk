# Account-local HTTP no-response recovery

## Production evidence

The diagnostic deployment on maker02 (hexbot `2055baba`, SDK `db0e952d`) reproduced the incident at 2026-09-29 04:51:29–40 UTC. A public WebSocket on `104.18.34.205:443` reset, followed by order, cancel, reconcile, heartbeat and replay HTTP failures on the same peer. Public standby promotion itself had no readiness gap. Five placements used pre-existing physical connections, wrote 1711/1712 plaintext request bytes, read zero response bytes, and reset after 1.8–3.2 ms. The last two placements occurred 9.5–10 seconds after the first order HTTP reset.

The recovery logs distinguish the actual blocker from a local backlog: repeated primary and secondary order queries returned JSON null, and 510 completed target cancels returned an empty `canceled` array plus `not_canceled[target] = "order can't be found - already canceled or matched"`; three other cancels failed in transport. None supplied an exact cancellation ACK. All five finally closed at 04:55:00.102–.147 after market end, a clean market cancel, absence, and complete historical no-fill auditing. Reservations remained held for 200–210 seconds. These responses do **not** justify early cancellation or releasing risk while the market remains active.

The separate cancellation-proof retention fix in PR #302 addresses a deterministic ACK-then-history-failure defect. It does not explain this production incident: no successful exact ACK was observed here.

## Repair and ownership

The existing account execution router retires only the failing physical slot for a single HTTP no-response error; a cluster can pause admission, but passing that pause does not replace idle siblings. A sibling can therefore be selected as a recovery probe on another stale socket. Extend the existing account-local reset path to fence all observed Fast/Cancel generations on the first new attempted order-endpoint request without a complete HTTP response.

The exclusive HTTP owner publishes a cumulative no-response count and its affected pool generation with the existing full health snapshot. Storage is allocated at startup. The counter persists across connection replacement and later successful requests, including several DELETEs within one cancel sweep. Duplicate completions are already attempt-fenced. GET warmups, local preflight/queue rejections and HTTP status failures do not produce this new evidence.

The router is the sole writer of retirement, pending reset and counter-consumption state. It consumes sequenced immutable snapshots from existing capacity-one replaceable lanes; monotonic counters preserve failure evidence on replacement. Retired generations cannot admit new places. Existing in-flight requests complete normally and preserve their individual order outcomes. No order/account/reservation state changes in this repair.

Reset commands carry the expected pool generation through existing capacity-one physical-owner lanes. Busy/full lanes retain one pending generation and retry without waiting; disconnected lanes remain unable to recover admission. A delayed command skips a newer generation. A delayed failure from an already retired generation cannot initiate another account reset. Fresh faults on replacement generations can. New prewarmed connections enter the existing bounded recovery-probe policy; GET /time alone does not establish business recovery. Cancels retain their existing emergency/fallback routes.

The fault evidence is account-local. Another account is unchanged, even when public market data share the same peer. No process-global network tracker authorizes trading. This can reduce repeated stale-connection submissions within an affected account; it cannot prevent a first ambiguous submission, a request already in flight when the fault becomes visible, or a new network failure after successful prewarming.

## Architecture and validation boundaries

No new worker, lock, queue, shared strategy account, allocation or synchronous log is added to steady-state quote/admission processing. Existing background runtime tasks rebuild and prewarm; the existing diagnostics worker logs the cumulative account reset count. All private order and fill lanes, owner identities and reservation rules remain unchanged. The pre-existing account connection router owns transport capacity, not strategy balances or risk; future extraction into explicit per-instance transport-budget messages must preserve that distinction.

Validation covers no-response followed by success, early replacement, duplicate and delayed failure snapshots, replacement-generation failures, bounded snapshot/command overflow retention, HTTP status rejection, slow-response isolation and account isolation. A focused owner-control benchmark measures bounded send/receive, observation and final slot-admission decision; it is not an exchange RTT or quote-to-fill benchmark. Production rollout evidence reports those separate boundaries and queue depth/overflow independently.

The existing saturated-cancel timing fixture previously included first-use telemetry slab allocation inside its 10 ms nonblocking-send measurement. A separate probe measured 12.734 ms on the first record versus 171 ns warm median (N=1000, P99 280 ns, max/P999 2581 ns). The fixture now prepares that telemetry stage before timing; the 10 ms limit and queue assertions are unchanged. Initial failing suite logs are retained.

Focused optimized-module benchmark (same actual admission source, `rustc -C opt-level=3`, local dependency artifacts; no HTTP or publishing-task work): 200,000 events per version, capacity/high-water 1, 3,125 intentional full-lane probes with retained observations, zero dropped observations. Before/after P50: 764/753 ns; P99: 1,092/1,130 ns; P999: 2,249/4,920 ns; max: 4,576,841/9,475,967 ns. This unpinned local run has scheduling tails (even its atomic-read control reached 6.816 ms); it does not establish an end-to-end latency improvement or rule out a tail regression. Production queue and end-to-end measurements are reported separately. Raw compilation arguments and results are retained in the hexbot rollout evidence directory.
