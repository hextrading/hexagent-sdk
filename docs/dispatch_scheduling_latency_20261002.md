# CPU-aware dispatch and reusable completion envelopes

The maker02 baseline (07:06:53.560–08:06:53.560 UTC on 2026-10-02) contains
8,093 completed place attempts. Local market receipt to HTTP future submission
was median 240.527 µs, P99 485.954 µs, P999 557.554 µs, maximum 604.216 µs.
204 of 835 three-order trigger groups prepared on a single Fast CPU. This is
observational evidence; the deployment comparison must stratify burst sizes.

## Ownership and scheduling

The execution dispatcher owns the CPU preparation schedule, issued counts,
completion watermarks, and each account's maintenance dirty flag. Its `Rc`
handles cannot cross threads. Each physical Fast owner publishes a cumulative
completion watermark through a startup-allocated capacity-one latest snapshot
lane. Superseded watermarks lose no completion credit. A guard completes exactly
once on preparation failure or immediately after dispatch, before waiting for
HTTP. CPU hints never include exchange RTT and never grant admission.

CPU binding is reserved at startup and reused by both the scheduler and the
worker. Healthy idle candidates prefer the least outstanding preparation work
on their CPU, preserving round-robin ties. Actual connection occupancy,
generation, freshness, probation, and final owner-local checks are unchanged.
No new workers, sockets, private-event routes, or strategy account writers exist.

Maintenance probes compact mailboxes and refreshes only dirty/changed accounts,
with a 1 ms deadline for expiry and retry processing. Capacity changes dirty
that account immediately. Health/reset/peer failures trigger maintenance without
waiting for the timer; safety cancels retain priority over ordinary cancels.
The destination account is still refreshed before each admission decision.

## Feed polling

`exchanges[].idle_poll_us` defaults to 100 and accepts 10–1000 µs. The consumer
sets only Binance and Coinbase to 25 µs. Empty adapters always sleep; no FIFO
busy spin or extra worker is introduced. CLOB and Chainlink keep their defaults.
CPU utilization and feed/router/private tails are checked during observation.

## Numeric signing and completion slots

The strategy parses each token into an immutable checked uint256 during market
registration, then copies it into its order messages. The signer checks original
decimal identity against the order symbol before using that word. Missing caches
(legacy callers or replay) use the validated compatibility path. Cache fields
are excluded from serialization. Numeric salt, amounts, and timestamp remain
numeric through hashing; serde writes the required decimal strings at the wire
boundary. Hashes, recoverable signatures, wallet identity, quantization and
zero/overflow checks retain compatibility, including POLY_1271 wrapping.

Each connection's instance route owns two startup-allocated completion slots.
Every slot has a bounded one-reply channel and preallocated timing metadata.
Checkout requires exclusive ownership of both slot and timing metadata after
all old endpoints/readers are gone. An acquire fence pairs with their final
reference drops. Abandoned I/O producers publish a transport failure; abandoned
receivers cannot let late replies cross into the next generation. Slot exhaustion
fails before place dispatch, or preserves cancel uncertainty for recovery.
There is no dynamic fallback on this live path. Legacy cold HTTP keeps its API.

Existing order/address Strings, HTTP task allocation, HMAC headers, and shared
adapter bookkeeping remain migration work. This change does not add a shared
account authority or new hot locks. A later owned wire-envelope migration should
remove those allocations together with registration/reply identity plumbing.

## Validation boundaries

Tests cover cross-account CPU selection, busy exclusion, changed-account reset
notification, all signature kinds and full-width token overflow, replay cache
omission, bounded completion exhaustion, abandoned endpoints, late replies, and
instance isolation. Existing private ordering/idempotence/reconnect/admission
regressions also run. `signing_latency` compares the compatibility and numeric
build paths with 20,000 samples per kind; `completion_slots_latency` compares
100,000 checkout/send/receive/release cycles in ABBA order. Both are local CPU
benchmarks with no network; live end-to-end tails require the one-hour rollout.
