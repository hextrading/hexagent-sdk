# Avoid token string copies during cold account reconciliation

A 45-second maker02 profile of the three cold account-owner threads (99 Hz,
68 samples) captured allocation/copying under lifecycle-mirror reconciliation.
The live owner-turn maximum remained 269 ms over the first 15 minutes. Sampling
identifies active work, not the full cause of that individual maximum.

Reconciliation builds its exact token union from borrowed strings and clones
only a nonzero residual retained in unallocated_positions. Pending settlement,
provisional ownership, risk checks and the complete token/account scope remain
unchanged. No new mutable field, lock, worker, queue, snapshot authority or
quote-path operation is introduced. Borrows remain inside the existing cold
owner state transaction and never cross threads. The temporary union still
allocates one hash table; this does not make historical full-account work bounded.

The focused benchmark uses 4000 tokens, two instances, 2000 events per arm,
queue depth/overflow 0/0. Boundary: exact reconciliation operation on a single
cold owner, excluding lifecycle queues/network/persistence. Original vs borrowed
P50/P99/P999/max in ns: 1643304/2274941/2967659/3454556 vs
950735/1451942/1994686/2088460. These are local release measurements, not live
end-to-end latency claims.

A separately measured exact-ended-set publication reuse candidate is NOT in the
implementation: its P99 increased from 556435 to 657508 ns and P999 from 688613
to 912566 ns. Reducing allocations alone did not satisfy the tail-latency gate.
The profile also captured ended_token_ids publication; a later owner-local
revision/scoped update design needs explicit invalidation and replay coverage.

Final functional validation after excluding snapshot reuse: 334 account tests
passed (16 ignored); locked workspace/all-targets check passed. No lifecycle
queue or ownership semantics changed; existing replay, overflow, idempotence and
isolation tests remain enabled.
