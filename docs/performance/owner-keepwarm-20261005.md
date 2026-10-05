# Permanent connection-owner maintenance

The maker02 restart-window review found 21 cold business requests at the end
of a four-hour scheduled trading pause. The scheduler reacquired each slot's
permit, but Fast, Cancel (including safety cancellation), and Reconcile actors
retain those permits for their lifetime. Startup prewarming therefore did not
establish steady-state maintenance coverage.

Permanent slots now lend the **same installed transport** to a low-priority
maintenance attempt. The actor retains its permit. A probe tries the existing
HTTP/1 request gate without waiting, and yields that gate when business arrives.
The business counter / active flag handshake is sequentially consistent, and
the cancellation waiter is registered before publishing active. This covers
both an already queued business request and arrival during a stalled GET.
Cancelled GET sockets close; a following business operation can incur the
normal, measured cold reconnect. This tradeoff prevents a 500 ms maintenance
response deadline from becoming business head-of-line blocking. No HTTP order
is replayed, and a probe never publishes a business admission/outcome proof.

The existing scheduler still requires 30 seconds of inactivity, a 500 ms
probe deadline, at most one probe per role pool and two globally. Business
completion now refreshes slot activity even when its actor retains the permit.
Busy maintenance is skipped and retried by the bounded sweep. Transport errors
retain existing generation-fenced quarantine and asynchronous repair. All
execution/private-event queues and their lossless/overflow behavior are
unchanged. There is no new thread, socket pool, shared account access, or
quote-path allocation. Per-slot atomic counters are exported by the existing
pinned `poly-admission-stats` worker, rather than formatting logs on order I/O.

## Validation

- Runtime: 116 passed, 10 ignored; engine: 176 passed, 10 ignored;
  exchange: 1008 passed, 54 ignored. Five connection routing/coalescing tests
  also passed after adding background coverage reporting.
- The permanent-permit regression reproduces legacy exclusion, then verifies
  maintenance coverage without releasing the actor permit or granting a
  business proof. Duplicate probes retain the per-pool/global bound.
- Loopback tests cover stalled-probe cancellation, closure of its old socket,
  correct DELETE response attribution, queued business priority, duplicate
  maintenance, and reuse across alternating maintenance/business requests.
  Existing tests cover generation retirement, failed repair, request deadlines,
  account isolation, lossless lanes, overflow, replay and idempotence.
- Focused release benchmark on the local x86_64 Mac, 1000 deliberately stalled
  probes, each interrupted by business (5 s artificial probe deadline):

| Boundary | Median | P99 | P999 | Maximum |
|---|---:|---:|---:|---:|
| Business HTTP gate wait | 15.478 us | 35.310 us | 62.080 us | 69.480 us |
| Gate through cold loopback response | 220.525 us | 386.513 us | 469.911 us | 3089.633 us |

Queue high-water was one, overflow zero, and all 1000 interrupted sockets were
closed and reconnected. These are local plaintext transport measurements, not
production TLS end-to-end numbers. The cold cost remains included in the second
boundary; it is not hidden by timing dispatch alone. Production validation must
compare receipt-to-dispatch, first-write, acknowledgement and private apply
separately, plus idle maintenance coverage, preemptions, reconnects and queues.
