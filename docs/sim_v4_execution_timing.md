# v4: execution stages inside the observed RTT budget

This is an offline SDK experiment. The original SDK pin, normal live runtime,
v2/v3 patches and prior result directories remain unchanged. No new worker,
cross-thread mutable state, synchronization or quote-path persistence is added.

## Configuration and compatibility

```toml
sim_v2_cancel_timing_mode = "staged_rtt_budget"
sim_v2_cancel_processing_ms = 75
sim_v2_execution_timing_audit = true
sim_v2_separate_taker_private_fills = true
```

The default mode is `legacy_l2_multiplier`. It retains the previous single
cancel `L2 * sim_v2_cancel_finality_delay_frac` timing, timeout behavior, and
legacy cancel-all behavior. Enabling the transition audit alone does not draw
random numbers or change economics. `staged_rtt_budget` requires explicit
independent private-fill and HTTP deadline handling; invalid configuration fails
at startup. The fixed processing parameter is an estimate, never a measured
venue latency. The old finality multiplier is ignored in this mode.

For total RTT `R = input_L1 + input_L2` and configured processing `P_requested`:

1. `P = min(P_requested, R)`; count every capped request separately.
2. `N = R - P`.
3. `L1 = round(N * input_L1 / R)` and `L2 = N - L1` (zero when `R = 0`).
4. Nominal arrival is `dispatch + L1`, cancel effective is `arrival + P`, and
   HTTP response is `effective + L2 = dispatch + R`.

Integer arithmetic conserves every ns, including asymmetric/odd splits. The
partition does not consume an HTTP or private-push RNG draw. It does not add
processing to latency2 RTT, which already includes server processing. Fixed
`P` is intentionally the first low-dimensional model; a conditional processing
distribution requires new independent stage evidence, rather than more free
parameters fitted to fills or PnL.

DES clocks remain monotonic. If a lane was already beyond its nominal arrival,
its actual application and subsequent stages move forward; audit retains both
the original nominal budget and actual processing time. Market feed events
retain the existing priority over ordinary server actions at equal timestamps.
HTTP responses exactly on the deadline win over their deadline timer.

## Observable stages and unknown times

Request arrival, exchange-side cancellation, HTTP delivery, and private fill
delivery are different events. A successful cancellation is generated only
after `core.cancel_order` removes the residual. A fill before that point may
produce a zero-economic matched HTTP resolution and an independently delivered
private execution. A partial-fill-then-cancel response retains the core's
authoritative matched quantity/trade IDs while private delivery may still be
in flight. Existing immutable trade IDs retain their deduplication semantics.

The adapter reads command `dispatched_ns/completed_ns` and transport timeout
labels. In staged mode `NewOrderTimeout`/`CancelOrderTimeout` marks a censored
HTTP observation, not a completed response: the adapter suppresses the invented
HTTP reply, while matching/cancellation continues at its modeled point. The
recorded duration is marked `right_censored_lower_bound`. It does not identify
the true total RTT or venue processing time; the RTT-partition point is still
an estimate. Legacy mode preserves its prior treatment for the control run.
Acceptance, rejection, private execution quantity and PnL are never inputs to
the timing partition or exchange matching decision.

The private fill sampler and missed-primary/reconciliation mechanism remain
independent. Actual matching is audited before any timestamp is overwritten by
delivery scheduling. Delivery is audited when the strategy scheduler actually
returns the private update, including earlier reconciliation recovery. A
legacy matched-cannot-cancel replay can repeat an immutable trade ID; compare
first match and first delivery of each `(iid, coid, trade_id)`, not raw row
counts. Staged mode removes that HTTP-to-private echo: matched HTTP responses
carry `Filled` with zero economic quantity, and the actual primary/recovery
private event remains independently scheduled. Even a censored HTTP response
cannot create a faster replacement private notification.

## Cancel-all scope and endpoint projection

The opt-in cancel-all path requires an explicit instance ID. Market-scoped
Polymarket commands require nonempty asset IDs; a missing market denotes that
instance's entire account. Scope is evaluated at **cancel effective time**:
orders admitted during processing are included if they are still resting in
the requested account/market. This membership rule is an explicit simulation
assumption, not recovered venue behavior. Other accounts/markets/venues are excluded, and member responses use stable
lexicographic coid order.

The endpoint has one RTT partition and one HTTP deadline. Its successful
response projects the resulting per-order updates together; it does not draw
a separate RTT per member. There is no public per-order cancel-all timeout DTO,
so a batch timeout is recorded as an HTTP audit event with empty `coid` and its
explicit `iid`; it does not fabricate an order's private lifecycle state.
Exchange execution still proceeds, and a late endpoint reply is discarded.
Enumeration is bounded to 1,024 inline order IDs before any member is canceled;
overflow or ambiguous ownership aborts the replay. Response-vector allocation
is confined to this existing offline emergency operation. Single cancels and
cancel batches also validate their explicit owner and immutable original venue
before staged dispatch, including retries after the resting order is terminal.

## Audit contract

Command replay writes `execution_timing_audit.jsonl` and embeds statistics in
`summary.json`. Full strategy replay writes `sim_execution_timing_audit.jsonl`
and `sim_execution_timing_summary.json` in its sandbox. All serialization runs
outside strategy callbacks in the offline replay loop.

Each row contains:

- `schema_version = 1`, `stage`, `coid`, `iid`, `token`, `trade_id`, `order_slot`.
- Simulator `request_id` for HTTP attempts; private executions join by trade ID.
- `actual_ns`: the time of this transition. `scheduled_ns`, when present, is a
  planned future delivery, not an already observed callback.
- `deadline_ns`, `status`, and `fill_quantity` where applicable.
- Optional cancel `timing`: original L1/L2/RTT, fixed/capped processing, network
  legs, nominal arrival/effective/HTTP times, and
  `evidence_confidence = estimated_rtt_partition`.

The stage names are `cancel_dispatched`, `cancel_arrived`, `cancel_effective`,
`http_scheduled`, `http_delivered`, `http_suppressed`, `http_late_discarded`,
`http_deadline`, `private_matched`, `private_scheduled`, and `private_delivered`.
`private_matched`/`private_delivered` describe economic fills, never ordinary
private order statuses. A cancel core result is not an HTTP acknowledgement;
use `http_delivered` to measure the client's observed response.

`command_arrivals.jsonl` adds original `attempt_id/iid/event_id`,
`sim_request_id`, mode, partition, `http_response_observed`, and
`rtt_observation`. Its legacy `cancel_finality_extra_ns` is zero in staged mode.
Place arrival evidence remains on its pre-existing estimated point.

Summary fields:

```text
execution_timing_audit:
  enabled, emitted, drained, queued, high_water, capacity, overflows
cancel_timing_stats:
  mode, processing_requested_ns, staged_requests, processing_capped_requests
replay_complete: bool
scheduler_pending_at_end: bool
```

The audit buffer owns 16,384 preallocated rows. Identities are inline, bounded
to 128 bytes; saturation or identity overflow aborts replay rather than losing
ownership or evidence. The single simulator thread owns all new mutable state.
The adapter finishes its fixed market-data window and drains outstanding
lifecycle work to closure without loading any future book. Completion requires
all commands consumed, market replayer exhausted, schedulers empty and audit
queue empty. A validator must independently parse the JSONL and require
`emitted = drained = parsed_rows`, `queued = 0`, and `overflows = 0`.

## Validation and performance

New unit coverage includes RTT conservation/asymmetry/capping, trade-before-
cancel versus post-effective no-fill, delayed private reports, successful
cancel/matched resolution semantics, response/deadline ties, censored HTTP,
timeout while exchange work remains scheduled, duplicate attempts, private
reconnect/reconciliation, instance/market isolation, and bounded audit overflow.
The existing broad exchange and engine suites remain required.

Run from the SDK worktree (builds are serialized by the root task):

```sh
cargo test -p hexagent-exchange staged_
cargo test -p hexagent-exchange timing_audit_is_legacy_economics_and_rng_neutral
cargo test -p hexagent-exchange --release staged_cancel_lifecycle_benchmark -- --ignored --nocapture
```

The benchmark performs 100,000 repeated cancel attempts on one terminal order
for legacy/audit-off, staged/audit-off and staged/audit-on. Its boundary includes
submit, server processing, HTTP/deadline delivery and bounded audit enqueue;
serialization/drain and external market matching are excluded. It reports
median/P99/P999/max, event count, scheduler steps, queue high-water/current/
capacity, audit counters, and HTTP deadline capacity. Existing DES heaps remain
growable, as in v3; measured queue capacities are reported, not described as
new hard bounds. This component benchmark is not an end-to-end live latency
claim. Full-strategy throughput and tail checks are a separate validation gate.

Calibration should keep the fixed command/event population, observed-versus-
censored HTTP distinction, false/missing fill quantities, maker/taker split,
per-order timing and admission coverage. Quantity MAE alone cannot justify
promotion: the previous low-finality experiment removed excess fills while
increasing missed shares and worsening full-strategy PnL error. None of these
results identify the unobserved true processing distribution or a unique
outbound/inbound split.
