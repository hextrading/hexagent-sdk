# Fast burst and order-I/O tail fix

Frozen maker02 evidence: 2026-10-04 13:42–16:35 UTC; latest complete evaluation 14:42–16:35 UTC. Live source was hexbot e02f94a with SDK 82dab5f, PID 2423431. Raw audit, quoter, logs and immutable-prefix hashes were synchronized locally before edits. This investigation changes no strategy admission policy or CPU allocation.

## Causes and boundaries

Twelve independent Fast connection owners share cores 5 and 14. Existing dispatcher-local per-core work hints spread the burst, but six orders still require three sequential preparations per core. One six-order receipt produced WS-ready→submit 433.867us and publication→owner dequeue 211.562us, with two sibling signature intervals on that core. Latest single-order owner queue P99 is 6.352us; six-order P99 is 188.788us. Prep→signed is median 53.965us, P99 57.115us, P999 58.507us (6,170 causal orders). Average CPU utilization does not describe this serialization.

Callback dispatch is independently measurable despite having no causal WS receipt. Latest cancel runtime-queue maximum is 799.046us (368 callbacks); a cancel-ack requote place reached 851.756us. These peaks follow several simultaneous connection-generation retirements/repairs; the preceding hour includes the reported cancel queue 1.822485ms. Synchronous client construction is only median 1.401us/P99 1.453us/P999 1.469us/max 1.470us (N=1,000) on maker02, so it does not explain the peaks. TLS certificate/handshake computation was still polled on the current-thread order reactor. There was no per-poll capture at the old 1.82ms event, so the exact old poll cannot be proven retrospectively. The fix removes that interference mechanism and adds cold-poll evidence.

## Change

V2 ordinary and POLY_1271 signing use an immutable, startup-owned, randomized libsecp signing context. Hashing, salt ownership, low-S, recovery byte, orderID, funder, and wire serialization remain unchanged. k256 0.13 and libsecp differ in RFC6979 prehash reduction for digest >= group order: preserve the original bytes with a rare k256 fallback. The comparison tests include group-order boundaries, zero/maximum digests, three keys, and 300 independent hashes, and recover each signature to the exact owner. Invalid key lengths now return startup errors.

Only a new connection's connector future moves to existing demoted order blocking workers. Workers use the original runtime handle, so TCP readiness stays registered with the order reactor and the completed TLS stream returns to normal order I/O. Reused connections pay no worker handoff. A capacity-one reply transfers the result; caller loss cancels the owned future. The entire cold connect, including worker pickup and TLS, has an absolute connect deadline. A stalled handshake cannot retain a worker indefinitely. SNI and certificate validation use the original unmodified hyper-rustls connector.

Cold work remains bounded by exclusive physical-slot gates, with one connector per slot and existing per-slot repair ownership; it does not create a new unbounded business lane. Existing hexbot-ord-bg workers retain background affinity/SCHED_OTHER and topology validation. Their poll samples use 64-entry lossy telemetry lanes, including CPU and wall durations for polls >=100us. Loss affects diagnostics only. HTTP audit adds nullable connect_worker_queue_ns, excluded from TLS and TTFB attribution. Slot generation fencing, complete-body prewarm, quarantine, one POST, idempotent DELETE hedge, and owner-local risk remain unchanged.

## Focused before/after measurements

Maker02 Neoverse V2; release fat LTO. Sequential read-only probes on housekeeping CPU 0, nice 19; synthetic cold workers CPU 1. No live order traffic. Startup, digest generation, sample storage/sorting and output are outside the measured calls. Full output is in docs/evidence/fast-core-io-tail-20261005/hexagent-arm-bench-final.log.

| Boundary / mode | N | Median | P99 | P999 | Max |
|---|---:|---:|---:|---:|---:|
| Prehashed signature, k256 | 30,000 | 47.575us | 52.613us | 60.383us | 181.384us |
| Prehashed signature, startup libsecp | 30,000 | 27.871us | 31.770us | 37.105us | 98.205us |
| Hot task enqueue→first poll, six inline cold polls | 2,000 | 1,801.032us | 1,801.772us | 1,806.688us | 1,829.227us |
| Same six cold polls offloaded | 2,000 | 7.190us | 73.060us | 117.111us | 215.650us |

Signing is synchronous: queue depth/overflow are zero. The synthetic interference test admits exactly six cold tasks per batch, then awaits all completions before the next batch; replies have capacity one and no overflow. Six is the submitted-work bound, not a sampled Tokio queue depth. Each cold poll deliberately burns 300us; this proves scheduler interference removal, not a live exchange latency distribution. Physical live owner queues remain exclusive (one business item), with existing overflow/fail-closed behavior. Background scheduling can lengthen cold setup; this is separately audited and bounded, rather than reported as TLS/venue latency.

## Validation

Runtime tests cover cancellation, worker failure, stalled TLS connect deadline, slot ordering/serialization, reconnect generations, reused socket timing, not-sent versus ambiguous transport, and failed/incomplete prewarm. Existing exchange/engine tests cover idempotence, replay, ownership isolation, overflow and private/lifecycle ordering. Real unauthenticated HTTPS GET on maker02 succeeded; the second call reused generation 1; IP hostname mismatch failed during TLS with zero plaintext HTTP bytes. Final suite counts and deployment measurements are recorded in the linked hexbot rollout report.

Reference API: https://docs.rs/secp256k1/0.31.1/secp256k1/struct.Secp256k1.html . The unreduced k256 behavior was confirmed against the locally locked ecdsa 0.16.9 hazmat/recovery implementation and exact byte comparisons.
