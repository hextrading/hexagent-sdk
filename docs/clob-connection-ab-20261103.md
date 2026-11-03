# Bounded live connection crossover

`HEXBOT_CLOB_CONNECTION_AB=account,start_unix_seconds,end_unix_seconds` opts
one account into a maximum one-hour window. Default is disabled. The absolute
end is retained across restart and the owner removes the expired policy.

Only Fast owners participate. Five-minute blocks alternate even/odd slots
between rotation and ordinary reuse. Rotation occurs between completed natural
strategy commands, after at least three distinct current-generation business
outcomes and at most once per five seconds per slot. All outcomes count equally.
Cancel/safety/recovery owners and other accounts are unchanged. No synthetic
orders, retries, additional workers or request queues are introduced.

Rotation enters the existing generation-fenced quarantine, background connector
and GET prewarm. It records repair cause 5 and the triggering attempt, with no
business failure and no invented business success. It uses normal DNS selection;
the analysis must stratify actual peer IP. Existing 500ms slow classification,
2s HTTP deadline, fresh business proof and fail-closed admission remain intact.
Already queued quotes can be rejected by the normal generation fence.

Each Fast owner solely owns the small schedule. No cross-thread schedule state
or new shared authority exists. The existing bounded repair/audit/health lanes
retain their priority, capacity, overflow and recovery behavior. New work is on
the connection-owner maintenance boundary, outside strategy quote processing.
An existing cold repair allocation and asynchronous connection setup happen only
when a rotation is requested. Existing affinity roles are reused.

Analysis must distinguish assigned arm from actual connection use. A fresh sample
is the first business request on a newly connected socket after GET prewarm;
it does not imply TLS happened inside that POST. Subsequent requests are reused.
Use account/role/slot/pool generation/socket generation/peer as identity. Include
timeouts, HTTP errors, every crossover block, normal repairs in both arms and
audit drops. Compare within peer and time block; preserve natural request order
when testing whether a slow socket stays slow. Record transport phases separately
from queue/preparation/lifecycle latency and health observations.

This tests client-to-edge connection dependence. Cloudflare IP and connection
generation do not identify an origin backend. Even a negative result cannot
exclude internal affinity, origin congestion or edge-to-origin delays. Diagnostic
rotation can temporarily reduce admitted capacity; this is part of the measured
experiment, not a reason to relax health checks. After expiry, observe ordinary
operation and verify that cause-5 rotations cease and real business recovery works.

Validation: owner policy tests cover duplicate/out-of-order observations,
generation changes, quarantine, crossover, expiry, and independent owner state.
Run the existing admission/routing suite for queue overflow and lifecycle fences.
`benchmark_completed` is a 200,000-event native focused timing sample with
median/P99/P999/max; this pure policy owns no queue. Record live stage quantiles
and existing queue metrics before and during the experiment as deployment evidence.
