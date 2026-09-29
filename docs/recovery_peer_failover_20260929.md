# Recovery after repeated failures at the same peer

## Production evidence

At 05:50:48 UTC on 2026-09-29, maker02 again observed public WebSocket and
order HTTP resets from `104.18.34.205`. The account generation retirement
introduced by PR #303 ran, and replacement clients completed prewarming.
Two subsequent POSTs to the same address still wrote their complete request
and received no response. They remained ambiguous for 247.963 and 244.092
seconds, until their market ended. Their 120 and 118 cancellation replies
never acknowledged the target cancellation. This deployment did not pass
the live convergence acceptance check.

Read-only checks then found `104.18.34.205 clob.polymarket.com` in maker02's
`/etc/hosts`. System resolution returned only that address, while a direct A
query to its configured DNS resolver returned both `104.18.34.205` and
`172.64.153.51`. The deployment must back up and remove this single host pin;
otherwise DNS-based failover has no alternative. The origin of that entry is
not established. Do not bypass arbitrary hosts policy in application code.

The previous HTTP audit's physical connection generation restarts at one for
every new client. It cannot alone distinguish an old client from a replacement.
The new `pool_generation` field records that missing identity alongside the
account, role, slot, attempt and physical generation.

## Change

An instrumented slot repair remembers the failed peer. When current DNS
answers include another address, its replacement excludes the failed address.
After three failed alternate prewarm attempts, it resumes ordinary DNS
ordering so an unreachable alternative cannot strand all slots. When DNS has
only the original address, existing bounded prewarm retry remains available.
Host, SNI, certificate verification and endpoint authentication
remain tied to the original hostname. No IP is hard-coded.

The exact no-response peer accompanies the existing cumulative fault evidence
to the account's transport admission owner. Its existing generation-fenced
refresh commands carry that peer to sibling slots, including when a command
lane is full. Late evidence from a previously retired generation cannot replace
the current recovery preference. Other accounts have separate preferences.
An independent private-stream reset has no HTTP peer hint.

This mitigates repeated requests to one failing edge. It cannot guarantee that
another address is healthy, prevent a later failure after a successful probe,
or resolve a POST that was already sent without an authoritative response.
No change interprets `null`, ambiguous cancellation, or elapsed time as a
terminal order proof. Reservations and private-fill ordering are unchanged.

## Ownership and latency

The exclusive HTTP completion writer publishes a fixed-size peer value with
the existing attempt evidence; the connection owner reads after completion.
The existing account transport router owns the recovery preference and pending
commands. Strategies keep their own accounts and order state. Existing bounded
snapshot and command queues retain their capacities, priorities and overflow
behavior. Private order/trade lanes are unchanged.

DNS filtering and its small address buffer run only while constructing a
connection on the existing background order runtime. There is no new worker,
lock, queue, blocking call or allocation in steady-state quote processing.
The advisory HTTP audit record grows by one `u64`, formatted and written by
the existing background audit worker. Full queues continue to count telemetry
drops without dropping private lifecycle events.

## Verification

Tests cover DNS alternatives, IPv4/IPv6, a sole DNS address, account isolation,
late and duplicate evidence, replaced snapshots, and full refresh-command
lanes. A local two-peer HTTP fixture reproduces `/time` success followed by a
POST with no response: an ordinary fresh client fails again at the same peer;
the repair preference selects the other peer and completes the POST. This is
controlled local evidence, not proof of production recovery.

Production rollout and at least 45 minutes of observation must follow, with
continued observation when a matching fault has not yet occurred. Report live
results separately from deterministic regressions and local benchmarks.

An optimized local admission microbenchmark used the actual before/after
module, 200,000 events each, bounded send → receive → full observation → final
slot admission. P50/P99/P999/max changed from 779/1092/3934/7806827 ns to
831/1167/2069/5900205 ns. Both runs had capacity/high-water 1, 3125 deliberate
full-channel attempts retained and zero dropped observations. This unpinned
local test includes scheduling noise (even its single atomic-read control had
a 3.97 ms maximum); it does not prove a production tail-latency improvement.
DNS/connect work and background audit formatting are outside this boundary.
