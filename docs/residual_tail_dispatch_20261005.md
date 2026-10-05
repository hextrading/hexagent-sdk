# Fast registration and causal private lifecycle tracing

The observed 148.896us Fast owner wait overlapped 139.856us of two earlier
same-core owner service intervals. The prior `account_recorded` timestamp
preceded runtime identity publication, two lifecycle messages, L2 authentication
and task submission. It did not mean all registration work had completed.

The new place path publishes one immutable ownership entry before POST and
passes that same Arc in one `RegisterPreparedOrder` message. The account owner
persists the prepared ownership and publishes identity plus open-order tracking
in a single snapshot. Full/disconnected registration rolls back runtime identity
and returns a preflight rejection before any HTTP request. Runtime live OIDs use
inline normalized storage and contiguous-byte hashing; oversized historical
identities retain exact normalization through a compatibility allocation.

L2 signing clones startup-keyed HMAC state and uses fixed 20-byte timestamp and
44-byte Base64 buffers. HTTP account/instance labels are startup-owned immutable
Arcs. `AuthHeaders.signature` and `.timestamp` now expose `ArrayString<44>` and
`ArrayString<20>` instead of `String`; callers constructing headers must adapt,
while HTTP serialization and signed bytes are unchanged. HTTP account labels use
Arcs, and `/order` borrows its static path. The current connection owner and
reply/generation semantics are unchanged. This does not remove every allocation:
prepared ownership, one immutable Arc publication, signing/body construction and
Tokio's request task remain candidates for the new measurements. The existing
crossbeam account lifecycle lane remains bounded at 16,384; this change replaces
two notifications with one and does not add a cross-thread request/reply.

`HotPathTrace` appends identity-published, registration-enqueued, attempt-bound,
L2-auth-complete and task-enqueued monotonic boundaries. Historical named and
compact records decode missing timestamps as unknown. Pooled request timing and
receipt-to-first-write measurement remain the downstream checks.

Private WebSocket sessions use `ReceiptSequencer`, carrying clock domain,
session, message sequence and frame event ordinal through `LifecycleTiming`.
The strategy worker stamps its actual dequeue, distinct from the private account
owner dequeue. An observation-only strategy hook receives the owner-local
lifecycle sequence after callback signals have been published. The direct
callback histogram stage is pre-registered at strategy-thread startup.

## Ownership and ordering review

- Receipt sequencing belongs to one WebSocket task and restarts with a new
  session identity on reconnect. Replay without a receipt stays unknown.
- Preparation timestamps belong to one connection owner. New trace fields have
  no admission authority. No new worker, risk gate or mutable global registry.
- Published ownership is immutable. The existing account owner alone mutates
  durable registration and execution snapshots. Instance and order generation
  stay inside the original ownership row.
- Business private-event delivery remains lossless and prioritized; added
  observation hooks must publish compact records without I/O or blocking.
- Combining workers per Fast core remains a separate migration: response waits
  must first become nonblocking pending-slot state. Merging the current blocking
  owner loops would serialize HTTP response waits across connections.

## Focused validation

Tests cover exact HMAC bytes across methods, UTF-8 bodies, empty/long keys and
u64 timestamps; old compact trace decoding; pre-POST publication and full or
disconnected rollback; inline/legacy OID normalization and owner isolation;
atomic identity/open-order snapshot publication. Existing replay, retirement,
private-event overflow, late fill and generation tests remain required.

Local x86_64 macOS release microbenchmarks use opt-level 3, LTO off and 16 codegen
units for both variants. They are not maker02 end-to-end or scheduling tests.
All times below are ns; P99/P999 are nearest-rank quantiles.

| Boundary / variant | N | Median | P99 | P999 | Max | Queue high-water / overflow |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1024-byte timestamp/HMAC/Base64, legacy | 100000 | 6974 | 7187 | 8244 | 60826 | 0 / 0 |
| Same, inline/template | 100000 | 6089 | 6257 | 7631 | 47174 | 0 / 0 |
| Prepared ownership→registration, two messages, six-entry batch | 36000 | 791 | 828 | 855 | 2958 | 12 / 0 |
| Same, combined message | 36000 | 524 | 555 | 569 | 14523 | 6 / 0 |

The registration comparison deliberately uses the new index for both variants
to isolate removed clones and handoffs; it excludes creation of the prepared
ownership, signing, consumer work, HTTP task scheduling and I/O. The higher
combined-path maximum is retained in the evidence, not discarded as an outlier.
Single/two/three-entry batches are also measured by the same ignored benchmark.
Production validation must compare matched burst groups and receipt→first-write
as well as submission, with queue overflow and private correctness checks.

Commands:

```sh
cargo test --workspace --lib --locked
CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 cargo test -p hexagent-exchange --lib --release residual_ -- --ignored --nocapture --test-threads=1
```
