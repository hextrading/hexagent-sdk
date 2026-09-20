# Live cancel, recovery and reservation latency correction (2026-09-20)

## Evidence and behavior

The Hexbot audit window is 2026-09-17 05:25:25 through 2026-09-20
17:02:04 UTC: 84 hourly files, 1,574,503 records. There are 442 overlapping
same-instance, same-order cancel intents among 201,124 direct cancellations.
The worst signal-to-preparation delay, 1,201.777 ms, occurs when two existing
cancels for one order occupy two of the three normal connections while another
order occupies the third. A third intent for the first order then waits locally.

* Coalesce an identical normal cancel while its original intent is queued or
  still awaiting a result. Preserve the first command and its lifecycle reply.
  Clear the pending-reply handshake before publishing the result so a retry
  caused by that result survives even before connection occupancy is released.
  Retain FIFO for distinct orders, explicit overflow feedback, and the separate
  safety-cancel lane. Export outbox depth/high-water/age/coalesced/overflow on the
  existing asynchronous admission diagnostic; record retained-command wait.
* Bind runtime reconnect audits to the existing execution-owner mailbox. The
  permanent connection owners cannot be borrowed through pool `try_acquire`.
  An absent/unavailable read now tries a different physical Reconcile owner by
  message, preserving instance identity and excluding the first slot. Valid
  terminal evidence follows the existing audit/private-finality path. Two null
  responses, unavailable owners, or wrong-slot replies never release risk.
* Extend the strategy-local incremental query cache to reservation totals.
  Replacements, partial fills, private fill/reversal, terminal removal and
  resurrection update the totals alongside the canonical pending-order map.
  Startup/restore rebuilds once; inventory adjustments prepare symbol entries.
  Querying cash and inventory no longer scans historical residual-zero orders.

The 247.1-second recovery incident began with an ambiguous send on an already
connected socket, followed by persistent null lookups until market expiry.
The fix restores a usable alternate lookup; it cannot promise quick resolution
when the venue supplies no authoritative order/trade evidence. Existing
replay fences and owner acknowledgements remain required before quote admission.

The measured HTTP tail is predominantly TTFB on warm sockets. Cancel HTTP
P99/P999 is 605.979/1,189.285 ms; place is 510.007/1,102.144 ms. 2,814 successful
cancels took over 500 ms. Keep the existing 2-second trading deadline: cutting
it to 500 ms would manufacture uncertainty for valid responses. No POST retry,
extra trading sockets, or relaxed risk gates are added. Venue/network TTFB still
requires production observation; these local changes do not remove it.

## Ownership and backpressure review

Reservation aggregates have the same sole strategy-thread writer as the ledger.
The dispatcher alone writes cancel keys, bounded FIFO membership and counters.
Keys include owner ID, full instance ID and COID; identities exceeding fixed
storage bypass coalescing rather than truncate. The key set is preallocated for
4,096 retained commands; lane-key storage is prepared during startup. No new
critical-lane lock, synchronous I/O, log formatting or cross-account authority
is introduced. Existing pending-order allocations remain; cache entries for
normal inventory are prepared before quoting. Legacy first-symbol insertion
and shared recovery/account compatibility state remain incremental-migration
work, not new shared authority for quote admission.

The existing 64-entry control mailbox is shared with RTT probes. Only the cold
recovery job waits for its one-entry reply. Full/disconnected mailboxes return
unavailable; a busy owner is not synchronously waited for by the dispatcher.
Ordinary and safety cancels retain separate queues and private/lifecycle events
retain their existing lossless higher-priority lanes. No new worker, affinity
role or scheduling policy is introduced. Recovery callbacks running on an
execution owner deliberately do not call back into the owner mailbox.

## Focused performance evidence

Release profile, rustc 1.97.1, x86_64-apple-darwin, Intel i9-9880H 2.30 GHz.
Each pair alternates old/new measurement order after warmup. Times below are ns.
These are local microbenchmarks, **not live end-to-end post-deployment results**.

Reservation boundary: private reservation mutation plus available cash/inventory;
excludes trade insertion, transport and scheduling. Queue depth/overflow: 0/0.

| Historical residual-zero rows | Version | N | P50 | P99 | P999 | Max |
|---|---|---:|---:|---:|---:|---:|
| 0 | scan | 20,000 | 69 | 71 | 79 | 97 |
| 0 | index | 20,000 | 69 | 79 | 85 | 92 |
| 1,000 | scan | 20,000 | 4,829 | 5,281 | 26,659 | 38,471 |
| 1,000 | index | 20,000 | 94 | 125 | 176 | 15,434 |
| 10,000 | scan | 20,000 | 66,539 | 110,045 | 175,297 | 224,620 |
| 10,000 | index | 20,000 | 144 | 218 | 449 | 19,836 |

Cancel boundary: dispatcher admission of A, B, A, A while three lanes remain
occupied. Excludes command construction, HTTP response and scheduling. N=40,000
each; overflow=0. The scan version retains previous dispatch behavior.

| Version | P50 | P99 | P999 | Max | Outbox high-water | Occupied owners |
|---|---:|---:|---:|---:|---:|---:|
| previous | 335 | 663 | 14,666 | 28,511 | 1 | 3 |
| coalescing | 416 | 490 | 1,489 | 26,462 | 0 | 2 |

The median admission cost increases by 81 ns to check exact identity; this
prevents duplicate intents from occupying downstream connections. This synthetic
benchmark does not estimate a new live maximum or simulate exchange latency.

## Validation and reproduction

After rebasing onto SDK main 0835fc6 (retired-order recovery), full
account/engine libraries: 321 + 134 passed, 13 + 7 ignored; full exchange library:
897 passed, 35 ignored. Earlier recovery-focused tests: 57 passed, 2 ignored. New tests cover FIFO,
coalescing, result-before-slot-release retry ordering, capacity overflow, owner
isolation, replay/reversal/restore, alternate-owner evidence, null evidence,
wrong-slot evidence and disconnected mailboxes. Existing RTT mailbox overflow
and reconnect fence tests are included in the recovery regression.

```sh
cargo test -p hexagent-account -p hexagent-engine --lib
cargo test -p hexagent-exchange --lib recovery
cargo test -p hexagent-account --release --lib private_reservation_history_latency_profile -- --ignored --nocapture --test-threads=1
cargo test -p hexagent-engine --release --lib repeated_cancel_admission_latency_profile -- --ignored --nocapture --test-threads=1
```

Raw audit summary, source file/line references, benchmark output and integration
results are maintained in Hexbot `docs/evidence/live_tail_fix_20260920/`.
Post-rollout acceptance must compare full-sample signal→prep, HTTP phases,
private receive→apply, queue high-water/overflow, and admission-paused durations.
No live process was restarted during this change.
