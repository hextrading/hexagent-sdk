# maker02 shutdown finality notification

During the 2026-09-27 rollout, PID 1473888 received SIGTERM at 10:49:50.053 UTC.
All three authenticated cancel barriers reported final by 10:49:50.152, but the
executor's next `coordinated shutdown barrier complete` message never appeared.
The standard restart script exhausted its 60-second grace period and used
SIGKILL. A read-only `/proc` snapshot before that deadline retained 80 threads;
the executor and router were runnable. They share CPU 4 at FIFO priorities 50
and 60. No userspace backtrace was captured, so a crossbeam wake-lock priority
inversion at the following bounded send is a hypothesis, not a proved stack.

Replace only this executor-to-router finality notification with a one-event
latched message. The execution owner publishes it with Release after its final
updates are enqueued. The router observes it with Acquire only after entering
shutdown, then runs the same lifecycle-tail drain and worker join sequence.
The existing <=10 microsecond live router poll supplies progress without a
crossbeam wake operation between those FIFO peers. A completion published before
the router processes Exit remains available; repeated BeginShutdown signals
cannot fill a queue. Sender disappearance without publication never means
finality. The remote cancellation barrier itself remains unchanged and unbounded
until authoritative evidence establishes safety.

The message has one startup-allocated slot, one execution-owner writer and one
router reader. No reset, overflow, dropped completion, new thread, CPU change or
steady-state allocation. The shutdown guard short-circuits the flag read while
normal quoting is active. It carries no trading/account authority. The existing
private/recovery and execution queues remain bounded and lossless; completion
does not bypass their drain or change per-instance routing or replay handling.
Other legacy crossbeam control lanes are outside this incremental migration.

Validation: release engine suite, 147 passed / 7 ignored. Regression coverage
includes early/duplicate completion, sender loss failing closed, independent
runs, publication ordering, real router shutdown with two owning strategies,
private replay, and final queued lifecycle updates applied before each owner's
shutdown report. Existing saturation/backpressure tests remain in the suite.
This is a shutdown correctness change, not a measured quote latency speedup.
Live startup recovery after the forced stop showed no ERROR or reconciliation
residual in the initial three-minute window. Final deployment observation and
any subsequent shutdown evidence are recorded in the hexbot rollout report.
