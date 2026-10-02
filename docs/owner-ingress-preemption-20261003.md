# Physical-owner ingress must yield to unpublished producers

The maker02 observation exposed a priority inversion at 2026-10-02 19:02:26 UTC. A Cancel owner (FIFO 55, CPU 4) spun in Crossbeam's array receiver while the execution producer (FIFO 50, CPU 4) was preempted in the reserved slot's message copy. The receiver could not observe a committed message until the lower-priority producer ran. The captured stack showed both sides of that publication. Capacity paused for up to 405.314 seconds; trigger-to-cancel dispatch reached 406.203 seconds even though P99 remained 0.357 ms. A 50 ms temporary boost of the producer to FIFO 56 released the queue; its original FIFO 50 policy was restored immediately. This was incident recovery, not the permanent deployment configuration.

Physical Fast/Cancel/Reconcile owner ingress now uses the existing preallocated `poll_channel` / `TryQueue`. The consumer returns empty on a reserved but unpublished head and sleeps between checks; it cannot spin indefinitely waiting for a preempted producer. Capacity, one physical owner per slot, FIFO reservation order, exact-message overflow feedback, retained cancel retries, generation fencing and instance identity are preserved. Every physical-lane test fixture uses this production queue, including duplicate cancel coalescing and isolation tests.

No worker, shared account authority or scheduling-policy change is added. The execution thread owns dispatch state; each connection owner alone consumes its queue. Sends remain bounded and nonblocking with no wake-lock, new allocation or logging. Physical owner receivers use a 50 microsecond requested polling sleep, including while a publication is incomplete; the generic polling channel default is unchanged. A maker02 synthetic scheduling probe pinned 12 FIFO-55 receivers and one FIFO-50 producer to spare CPU 0, with its bounded controller on CPU 1. Over two seconds, 10 us polling consumed 99.851% receiver CPU and allowed only 589 producer turns; 25 us used 48.200% / 17,746 turns, 50 us used 24.085% / 19,082 turns, and 100 us used 12.801% / 19,526 turns. The producer requested a 100 us sleep. This idle scheduling probe omits real message processing; full production CPU and wakeup latency still need the next live observation. Same-thread queue benchmarks do not measure those costs. Private lifecycle/completion semantics are unchanged.

Validation: 171 engine tests passed (9 existing ignored), 95 runtime tests passed (6 existing ignored). A new deterministic timeout test holds the FIFO head before publication, verifies the receiver can return at its deadline, preserves later message order and full-queue ownership, then checks disconnect after draining. Existing tests cover retained retries, duplicates, instance isolation, replay and recovery.

Local x86_64 macOS release microbenchmarks, 100,000 events each, nearest-rank tail percentiles as emitted by the existing harness:

| Boundary / implementation | Median ns | P99 ns | P999 ns | Max ns | Queue high water / overflow |
|---|---:|---:|---:|---:|---|
| try-receive to independent work, 200 injected 10 ms holds, Crossbeam | 109 | 156 | 10,191,259 | 15,352,316 | 1 / 0 |
| Same injection, poll channel | 87 | 117 | 3,249 | 44,884 | 1 / 0 |
| Prebuilt push + pop on same thread, Crossbeam | 106 | 160 | 301 | 39,775 | 1 / 0 |
| Same-thread poll channel | 100 | 140 | 319 | 37,677 | 1 / 0 |

The injected test measures returning from an incomplete publication, not completing that unpublished message. These local tests are not a substitute for Linux FIFO scheduling validation or full trigger-to-HTTP measurements. The next live run must start a fresh 60-minute observation, retain maximum latency and every capacity outage, and report per-core CPU and queue statistics alongside P99/P999.
