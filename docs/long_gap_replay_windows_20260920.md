# Long-inactive account replay through bounded time windows

On maker02, zhu03's persisted replay anchor was 1787214778 (August 20),
while startup was September 20. Authenticated whole-account GET /data/trades
with after=1787214777 repeatedly returned HTTP 500, including an explicit
initial cursor. The same query with a one-day upper bound returned three rows
and the terminal cursor. A read-only walk of 32 contiguous daily windows
returned HTTP 200 and a terminal cursor for every window: three rows total,
10.722 seconds of HTTP time. No account state was modified by that probe.

Replay now bounds old history to one-day windows, preserving account-wide
coverage. The documented before/after parameters are also used by the
[official client](https://github.com/Polymarket/py-clob-client-v2/blob/main/py_clob_client_v2/client.py).
Adjacent windows overlap at their boundary second. Existing durable trade
deduplication and instance ownership routing handle the overlap. The last
window remains unbounded above, preserving the current live catch-up behavior.

The original anchor and initial wall-clock horizon stay pinned throughout a
sweep. Each window retains its exact cursor and partially acknowledged page on
failure. Only a terminal page acknowledged by the private owner may advance
the window. Cursor rejection resets only the current window. HTTP, parsing,
owner-delivery or finality failures still keep recovery admission closed; no
checkpoint, reservation or ledger is cleared to force admission.

The new two integer fields belong to the existing cold replay task. There are
no new workers, shared mutable state, queues, locks or quote-path operations.
The existing private apply lane retains its bounded capacity, ordering,
backpressure and generation fences. A restart may replay old windows again;
durable idempotence remains the authority, not an in-memory completed count.

Validation includes boundary coverage, recent/future-clock behavior, retaining
an unacknowledged terminal page, cursor retry, delivery/reconnect, duplicate
trade application, instance isolation and overflow through the existing full
exchange suite. The read-only HTTP walk is evidence for recovery availability,
not an end-to-end latency benchmark. Production acceptance still requires both
instances quoting and a fresh 30-minute window with request percentiles and
queue depth/overflow.

Results: gap-focused tests 34 passed; full exchange library 900 passed,
35 ignored, 0 failed.
