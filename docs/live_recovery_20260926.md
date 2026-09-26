# Live cancellation recovery and CLOB allocation fixes

A transport failure during DELETE previously returned Accepted without a durable
cancel-recovery intent. Four unknown placements also kept using the legacy
per-order lookup path until expiry, despite the account reconnect path having
an exact-cancel/history recovery implementation.

## Behavior

- Transport, malformed-response and locally-unsent DELETE failures retain their
  original error and enter cancel recovery. No reservation is released by error
  classification.
- The live per-instance Reconcile command goes to a cold coordinator, then sends
  HTTP legs to physical Cancel/Reconcile owners. Unknown placements with absent
  GET results require an exact cancellation ACK and complete authenticated trade
  history before release. Late fills revalidate identity at the durable owner.
- Private WS disconnect publishes an account-local reset intent. The dispatcher
  retires existing Fast/Cancel generations, requests replacement on idle owners,
  and uses the existing recovery admission state machine. Busy owners retain a
  pending reset; old heartbeats/ACKs cannot re-enable retired generations.
- Active and candidate CLOB readers preallocate frame event/diagnostic/repair
  buffers once and drain/reuse them. Canonicalization borrows the token role and
  updates a warm quote-version key without cloning role strings.

## Ownership, pressure and migration boundary

Each strategy retains sole write ownership of its StrategyAccount and local
orphan state. New coordinator threads are instance-specific, SCHED_OTHER, pinned
and registered using pin_background. Their input mailbox has capacity 1 plus
one executing command; overflow/disconnection sends exact-owner typed deferred
feedback through the existing lossless lifecycle lane. The strategy retains the
orphan. HTTP uses the existing bounded owner request lane, with cancellation
routing independent of place admission. No physical HTTP actor waits on itself.
Shutdown disconnects pending HTTP requests before joining coordinators/owners.

Reset mailboxes have capacity 1 per physical account and coalesce equivalent
intent. Dispatcher-owned preallocated pending bits retain busy-lane work. No new
strategy quote-path locks, I/O, logging, account reads or queues were introduced.
A disconnected reset producer is possible only during teardown; producer and
consumer handles are startup-bound.

Frame buffers belong solely to their reader task. Typical live frames publish
at most 4 events; initial event capacity is 256. Existing large legal frames may
adopt/grow a larger adapter buffer, retained for subsequent frames; no event is
silently dropped. This is an incremental removal of observed per-BBO Vec growth,
not a claim of a fully allocation-free parser: owned wire symbols, Decimal
conversion and full book payloads remain. A future symbol-ID/fixed-capacity wire
migration must preserve overflow and full snapshot semantics. Legacy shared SDK
recovery/audit machinery remains cold; it is not a strategy admission authority.

## Verification

Functional coverage includes real engine dispatch, full/disconnected coordinator
mailboxes, instance isolation, busy owner reset retention/coalescing, retired
generation fencing, reset DELETE recovery, exact ACK, complete/incomplete history,
late fills, duplication, reconnect and shutdown. Existing allocation-free
quantity-update tests remain in force; one-shot compatibility parsing retains
empty default buffers while live readers explicitly preallocate.

Controlled release benchmark compares former empty-per-frame buffers with reused
buffers; canonicalization is held identical in both arms. It measures input copy,
parse, canonicalization, batch creation/reuse and event drain on one thread, not
network or strategy dispatch. Production end-to-end tails require post-deployment
observation and cannot be inferred from this microbenchmark.
