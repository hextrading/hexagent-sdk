# Bounded strategy startup lifecycle handoff

The live strategy worker can pause all three existing lifecycle receivers when
an owner-local startup FIFO is full. `startup_lifecycle_intake_paused()` is cached
at startup and after lifecycle/watchdog callbacks. Market and quote callbacks do
not poll it. Receivers switch by reference to `never` receivers created before
the loop; no new worker, production queue, lock, allocation or shared-account
lookup is added to quote processing. Control/history/watchdog callbacks remain
available to finish initialization and drain the FIFO. Shutdown drains also
respect the cached pause.

The consumer must retain the envelope that fills its capacity before returning
pause. This API provides bounded backpressure, not unlimited losslessness. The
Polymaker handoff uses 1,024 preallocated entries and at most 32 applications per
watchdog turn, stopping after any update emits signals. Market readiness remains
closed until the owner account, target event PM and buffered lifecycle work are
ready. The app separately checks immutable trade-row/status coverage before
committing a deferred recovery acknowledgment; `Ok` or a zero signal count alone
is not proof of application.

`DeferredRecoveryUpdateAck` captures the **receipt-time current recovery
generation and exact update key**. It is not cloneable and has no Drop ACK.
`commit()` sends a one-way completion on the existing bounded recovery-owner
lane. Queue saturation retains the recovery obligation and marks inventory
uncertain. Buffered messages cannot consume the same key registered in a later
epoch after receipt. `LifecycleEnvelope` does not carry the producer's recovery
generation, so this is not an end-to-end producer-epoch guarantee for arbitrary
messages already in upstream queues when epochs change; the existing routing
and recovery ordering contract still applies there.

A completely saturated chain can couple seed readiness to a terminal private
update that has not yet reached cold accounting. The ordinary two-second direct
send timeout requests recovery, but does not prove all such cycles self-resolve.
The app therefore reports a terminal startup failure after 60 seconds without
seed/application progress while it holds pending messages. This is checked only
on watchdog callbacks. `startup_lifecycle_failure()` causes explicit instance
quarantine, an emergency cancel on the existing control lane and suppression of
any partial signal batch. The existing strategy thread retains its FIFO/tokens
and waits for controlled shutdown, responding to `shutdown_requested`, market
Exit or receiver disconnect. The supervisor continues cancellation retries.
Operator/script restart and durable replay are required; this is not automatic
recovery and no unapplied update is ACKed. No progress timer is added to ordinary
completed startup or quote processing.

Focused regressions use the real strategy worker for direct/compatibility
backpressure, FIFO sequences, instance isolation, market/watchdog progress,
shutdown drain and terminal quarantine. The third private-feed receiver is
covered by the same production selector with its actual input type. Deferred
ACK tests cover drop-without-commit, later epochs, sibling instances, duplicates
and ACK overflow. A real private apply worker test fills its capacity-1 direct
lane, exercises the production two-second timeout and reconnect notification,
then injects the authoritative replay fixture through the real replay/cold path
and verifies one-time economics and explicit recovery completion. It does not
contact an exchange or prove remote reconnect success.

A focused alternating old/new benchmark measures three already-ready lifecycle
receiver selections through dequeue, with enqueue outside timing, N=100,000 per
version, queue depth 1 and zero overflows. Old/new P50/P99/P999 are
117/200/234 ns and 117/199/235 ns; maxima are 19,981 and 23,012 ns. Both the higher
new maximum and the lack of causal attribution are retained. The harness uses
exact production selector and benchmark sources with rustc opt-level 3 and the
existing debug crossbeam rlib. It excludes callback execution, scheduling
wakeup, account application and network; it is not a production E2E claim.

Final SDK library validation: engine 114 passed / 5 ignored / 0 failed; exchange
826 passed / 27 ignored / 0 failed; account 308 passed / 10 ignored / 0 failed.
The account suite was run separately for the coherent cold startup-seed API.
