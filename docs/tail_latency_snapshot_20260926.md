# Live tail latency and coherent reconciliation, 2026-09-26

## Evidence and measurement limits

The previous maker02 30-minute window (12:51:28.680–13:21:28.680 UTC) had
market receive → router max 87.240 ms, private WS receive → strategy trade apply
max 8.430 ms (JSON parse max 8.370 ms), and `polymarket.account.owner_turn`
max 243.130 ms. That last metric belongs to the **private lifecycle owner**;
it is not the cold account worker's exclusive processing time.

A new 270-second, 199 Hz cycles profile of the three private owner TIDs on the
unchanged live binary caught `register_token_fee_config_with_settlement` inside
`AccountOwnerCommand::execute`, cloning `SharedAccountState`, taking both
persistence images, copying all virtual lifecycle rows, and rebuilding routes.
The corresponding 14:34:35 UTC export reported another 214.70 ms owner turn.
The call repeats when registering the next five-minute market. The profile
supports removing this call path; it does not prove that every historical
market/JSON tail had the same cause.

## Changes and ownership

* New-token fee registration with no existing executions remains serialized on
  the private lifecycle owner. It publishes the immutable registry and sends
  only token/config keys through the existing ordered lifecycle mirror and WAL.
  Duplicate registration is a no-op. An overflow returns an error and retains
  fail-closed admission. Cold aggregate publications cannot overwrite a newer
  registry while its message is pending. Existing-execution revisions retain
  the provenance-freezing fallback, followed by an ordered registry mirror.
* Register/retire/prune token interests persist exact map entries rather than
  before/after images of the whole ledger. Read-only polls schedule no WAL.
* Aggregate reconciliation skips hashing zero-valued historical positions.
  Signed, cancelling, and pending positions still participate; historical
  account data is retained. Mirror-only updates reuse ended-token/fee registries.
* Trade mirrors now publish economics immediately. Wallet balances and the
  maintenance map reuse immutable roots until a real cold mutation changes them.
  Monitoring has an aligned physical/virtual/unallocated/pending snapshot,
  capture time and ledger generation. Pending MATCHED cash/shares are explicit.
  Per-token reconciliation avoids cancelling unrelated token discrepancies.
  Live reservation/instance observations remain separate diagnostic values.
* Private JSON parses the original input and moves array children into the
  existing bounded apply lane. Malformed data still forces recovery. New CPU
  and wall-minus-CPU counters distinguish parser work from descheduling.
* Market messages carry a monotonic queue enqueue timestamp. Per-venue adapter
  and root queue ages supplement, rather than replace, the original wall-clock
  receive age. Private owner command/execution/ingress/maintenance/retirement
  stages locate any remaining owner tail. The fixed telemetry slab is 2 MiB per
  recorder, preallocated before each worker's steady-state processing.

No worker, CPU placement, queue capacity, market routing or private ownership
rule is changed. The lifecycle mirror capacity remains 65,536; its single
producer preserves watermark order and any gap/overflow keeps admission closed.
The existing bounded persistence queue retains generation-ordered replay.
Market ordered records retain FIFO/reconnect-on-overflow semantics; replaceable
observations retain their bounded oldest-replacement policy. Stamps are scalar
fields, without additional queue allocation or producer I/O. Histogram export
and diagnostic formatting remain on the existing background workers.

The pre-existing aggregate fallback for revisions that already have executions
still clones history on the lifecycle owner. Migrating that rare path requires
an owner-local provenance-freeze message followed by asynchronous cold commit;
this change does not weaken its replay guarantees to optimize it speculatively.
The general runtime still allocates owned private JSON before transferring it;
this is outside the strategy quote path and allocations are measured below.

## Local validation

macOS release, single test thread; other local compilations were running.
Numbers are individual samples in nanoseconds, not production percentiles.

| Parse + array handoff + drop | N | P50 | P99 | P999 | max | allocations / bytes |
|---|---:|---:|---:|---:|---:|---:|
| previous simd input copy + subtree clone | 20000 | 6854 | 28731 | 32418 | 37089 | 102 / 12198 |
| resident simd buffers + move | 20000 | 3496 | 3728 | 27969 | 63340 | 47 / 4650 |
| original input serde + move, selected | 20000 | 3334 | 4780 | 24615 | 31860 | 46 / 2466 |

This isolated parser benchmark has no queue (depth/overflow 0/0). The existing
market consumer benchmark with queue stamps recorded N=100000,
P50/P99/P999/max=124/214/246/6874 ns, boundary `try_recv` entry → returned event;
queue high-water 1, final depth 0, contention drops 0.

The initial coherent-snapshot implementation was rejected: mirror publication
on 4,000 zero historical tokens regressed P50 from 361311 to 644731 ns and P99
from 656758 to 1035856 ns. It unnecessarily rebuilt unchanged physical maps and
hashed zero balances. Final measurements follow after those costs are removed.

Regression coverage includes typed WAL register/retire/prune/restart, no-op
polls, pending MATCHED→MINED accounting, mirror publication/instance isolation,
new fee registration→trade→revision ordering, replay, duplicate registration,
mirror overflow including duplicate retry, signed/cancelling positions, and
JSON corruption/Unicode/order preservation. Existing private routing,
reconnect/replay, queue overflow and account fee-provenance suites remain in use.


Final release comparison, N=2000 per row, 4,000 historical zero-valued tokens:

| Boundary / implementation | P50 ns | P99 ns | P999 ns | max ns |
|---|---:|---:|---:|---:|
| position residual recompute / previous union | 629921 | 1048048 | 1412536 | 1428240 |
| position residual recompute / skip zero keys | 14411 | 35232 | 55634 | 88936 |
| persistence capture+drop / full before+after | 1453504 | 2141685 | 2946168 | 3325202 |
| persistence capture+drop / exact interest entry | 978 | 21237 | 39562 | 79394 |
| mirror publication / previous registry rebuild | 367678 | 633328 | 883694 | 897266 |
| mirror publication / reuse + coherent economics | 13454 | 33630 | 53069 | 53381 |

These are synchronous focused boundaries with queue depth/overflow 0/0;
persistence capture excludes the asynchronous writer. The new mirror row
includes constructing and publishing coherent reconciliation. Zero-heavy
synthetic history does not predict active-position cost or production maxima.

`cargo check --locked --workspace --all-targets` passes. Relevant library suites:
account 340, exchange 928, engine 144, runtime 76 passed (1,488 total), including
existing ordering/overflow/replay coverage; ignored live/manual benchmarks are
not claimed as executed. Production 30-minute validation is recorded by the
consumer deployment report after the merged SDK revision is pinned.
