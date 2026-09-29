# Early CLOB peer failure notification

At 08:30:51 UTC on 2026-09-29, the public CLOB socket to
`104.18.34.205` reset about 198 ms before a btc03 POST. A btc01 recovery GET
then failed on the same address about 76 ms before another POST. Both POSTs
wrote their request and received no response. Their order GETs returned null
and repeated DELETEs returned an ambiguous canceled-or-matched reason. They
closed only after market expiry, after 248.620 and 248.081 seconds.

The prior repair did avoid the failed address after the first business failure:
replacement logs show `avoided_peer=104.18.34.205` and
`peer=172.64.153.51`. The missing edge was earlier evidence: public WS failures
were only diagnostic, and cold HTTP failures repaired only their own socket.

## Behavior

- Each account adapter creates a bounded notification mailbox at startup. Its
  execution dispatcher claims the only receiver. The live feed receives an
  immutable list of senders for enabled Polymarket execution accounts.
- A hard TCP read error on either public WS lane publishes the measured peer.
  Normal closes, server slow-consumer closes, timeouts and malformed messages
  do not produce this signal. Cold account HTTP publishes transport failures
  with a known connected peer; slow GETs, HTTP status errors and invalid JSON
  do not contribute business-success or failure scoring.
- The dispatcher consumes notifications before admitting the next POST. It
  retires the currently observed order generations and uses the existing
  bounded refresh commands and background DNS peer avoidance.
- An account-local peer hint lasts ten seconds for a subsequent private-feed
  reset. This prevents the private reset from immediately discarding the
  earlier public/HTTP evidence and rebuilding onto the same address. It does
  not establish a permanent address preference or change DNS authority.
- Duplicate and older notifications do not retire an already repaired
  generation. An occupied/full connection command lane retains the exact
  generation and peer hint until it can enqueue the refresh.

## Ownership and backpressure

The mailbox has 16 FIFO entries plus a capacity-one overflow reset intent.
Public readers and cold HTTP tasks are nonblocking producers. The existing
execution dispatcher is the sole consumer and sole writer of deduplication,
recent-peer and retirement state. Each poll drains at most 16 entries and
coalesces the batch into one retire-all action, with the newest peer hint.
Overflow retains a retire-all intent even if its address hint cannot fit.
This is transport evidence, not a lossless order/private-event lane.

There are no new threads, shared risk gates, global maps, locks or steady-state
quote allocations. The regular additional dispatcher work is bounded empty
mailbox polling. Diagnostic formatting remains on the existing background
diagnostics worker. Existing affinity/topology assignments remain applicable.
The legacy aggregate account implementation is not extended as a runtime
authority; further migration remains owner-message based.

`execution_peer_failure` diagnostics record the source, peer, source timestamp,
notification-to-owner retirement latency, capacity, overflow and reset count.
Actual HTTP traces retain logical pool generation and socket peer, allowing a
production incident to be joined through subsequent request dispatch.

## Safety boundary

This prevents a later request from using an old connection when earlier peer
failure evidence has reached its execution owner. It cannot undo a POST already
in flight, or prevent the first ambiguous POST when there was no earlier
signal. Unknown order reservations, exact cancellation proof, complete fill
history requirements and expiry recovery are unchanged. It neither repeats
an unknown POST nor infers zero fills from null GETs or retry counts.

## Validation

Tests cover the live WS/GET-before-POST ordering, exact instance feedback,
independent accounts, public subscriber fan-out, duplicate/reordered events,
repaired generations, mailbox overflow, occupied connection owners, private
reconnect after public failure, and peer-hint expiry. A local TCP server verifies
that a received GET followed by EOF publishes the actual peer while ordinary
HTTP and JSON errors do not.

The focused benchmark uses the actual admission and mailbox modules compiled
with `rustc -C opt-level=3`. Its boundary is bounded owner-observation
send/receive, full observation application and final slot admission; the new
version also polls an empty peer-failure FIFO and overflow lane. Setup, HTTP,
socket repair and strategy callbacks are excluded. The local machine is
unpinned, so its scheduling tails are not evidence of production improvement.
Validation on the final code: engine library 156 passed / 8 ignored; Polymarket
exchange tests 493 passed / 30 ignored (490 unrelated tests filtered); engine
library check passed. Existing ignored tests remain ignored.

| Owner admission + empty message polling, ns | N | Median | P99 | P999 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before (`06b0d44`) | 200,000 | 840 | 1,209 | 1,826 | 3,378,064 |
| After | 200,000 | 878 | 1,320 | 1,981 | 3,281,710 |

The added polling costs 38 ns at the median in this focused sample. Existing
health-observation capacity/high-water is 1/1; all 3,125 deliberately saturated
publishes remain retained, with zero dropped observations. The new mailbox is
empty in this measurement (capacity 16 + 1, high-water 0); separate overflow tests
fill all 16 entries and preserve the fallback reset. This is not a measurement
of full dispatch-wrapper or end-to-end latency. Production stage distributions,
queue counters and incident ordering must be checked after deployment. Initial
samples taken alongside test compilation are preserved separately from this
final sample.
