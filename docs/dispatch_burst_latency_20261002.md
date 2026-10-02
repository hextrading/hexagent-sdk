# Bounded direct signal ingress and parallel signing

The maker02 baseline contains 7,319 completed place attempts in the hour
2026-10-02 03:20:57.091–04:20:57.091 UTC. Local market receipt to HTTP-future
dispatch was median 271.523 µs, P99 615.058 µs, P999 812.808 µs, maximum
844.942 µs. Signal emission to preparation was P999 583.078 µs. The slowest
Coinbase trigger produced six SELL orders across three instances, dispatched
at 339.652, 452.864, 550.678, 649.359, 748.590 and 844.942 µs. All twelve Fast
owners shared CPU14; their preparation intervals never overlapped.

## Ownership and channels

Live execution consumes each instance's existing 1,024-command lane directly,
without the signal-arbiter forwarding thread. These lanes and the system lane
use bounded nonblocking polling publication. Each instance remains the sole
writer of its StrategyAccount and reservations; execution receives owned
commands and returns lifecycle results through the existing owner-stamped
lossless lanes. Already-reserved commands are never coalesced or dropped.

Only the execution thread owns ingress cursors, disconnect state and a pending
control barrier. Normal system/instance lanes are round-robin. The independent
eight-message control lane remains available after ordinary queue overflow.
Pending controls drain already-admitted commands before advancing; an
unfinished FIFO publication cannot be overtaken by a barrier. An unfinished
control publication yields to its producer rather than running ordinary orders.
Full strategy publication retains the existing quarantine/emergency-cancel
behavior. Cold system producers may retry bounded publication; quotes never
wait for capacity. Public compatibility entry points and paper keep their
existing receiver API and arbiter.

Queue occupancy is sampled by existing 1 ms execution maintenance. Compact
30 s diagnostics go to the existing background writer; sampled high water is
not an exact queue maximum. No logging, formatting, persistence or new worker
is introduced into the quote callback. Private/lifecycle priority is unchanged.

## CPU placement

The consumer configuration can place Fast owners on two isolated physical
cores without changing the connection count. New explicit, default-off options
allow SCHED_OTHER private cold writers onto configured background CPUs and
short Cancel owners beside the dispatcher. Cancel must have higher FIFO
priority than the dispatcher and lower priority than a co-located market router.
Fast and completion still cannot share those critical CPUs. Cold sharing also
requires the existing shared-cold/background opt-ins. Topology validation rejects
missing opt-ins, priority inversions and overlaps with strategy/private owners.

## Signing

Immutable maker/signer words are parsed at signer construction; funder changes
update the cached maker word. Built v2 orders use these words and the already
validated builder bytes. Arbitrary prebuilt orders and alternate POLY_1271
wallets retain their own identity. Hash input/digests use fixed stack storage;
decimal-to-uint256 conversion does not allocate a digit vector. Hex output
reserves its exact capacity and avoids intermediate strings. Protocol digests,
signature layouts, validation, salt uniqueness and quantity rounding are intact.

Existing wire-order Strings and completion envelopes still allocate. This
change removes intermediate allocations rather than expanding that legacy
surface. A later migration can carry typed immutable token/address fields and
preallocated wire envelopes; it must preserve owner identity and publication
rollback before replacing existing commands.

## Validation

- Config: 9 passing tests. Runtime: 90 passing, 6 manual benchmarks ignored.
- Engine: 165 passing, 9 manual benchmarks ignored. Includes direct-ingress
  instance order, overflow, independent emergency control and actual coordinated
  shutdown; existing private priority, replay and isolation tests remain green.
- Exchange: 983 passing, 51 ignored. Includes fixed protocol vectors, full-width
  uint256 reference comparison and alternate-wallet digest/signature checks.
- Focused signing example: 20,000 events per signature type per run, on macOS
  x86_64 without affinity, boundary `build_signed_order_dispatch`, no transport
  queues (depth/overflow 0). Alternating before/after/after/before runs:

| Variant/type | Median µs | P99 µs | P999 µs | Maximum µs |
|---|---:|---:|---:|---:|
| Before EOA run 1 | 126.726 | 187.198 | 272.932 | 776.807 |
| After EOA run 1 | 121.227 | 171.184 | 228.608 | 352.014 |
| After EOA run 2 | 126.124 | 195.502 | 339.848 | 1773.136 |
| Before EOA run 2 | 131.265 | 184.042 | 284.933 | 1471.775 |
| Before POLY_1271 run 1 | 134.886 | 186.426 | 260.558 | 2351.953 |
| After POLY_1271 run 1 | 131.167 | 207.872 | 389.068 | 1774.965 |
| After POLY_1271 run 2 | 130.174 | 202.000 | 390.972 | 1582.122 |
| Before POLY_1271 run 2 | 138.921 | 203.614 | 314.784 | 1398.290 |

Median CPU cost improves slightly; these unpinned samples do not establish a
tail improvement. Deployment validation must measure local trigger through
dispatch and the first observed plaintext transport write/flush, with burst
size stratification, queue backpressure and private-event correctness. Dispatch
is the HTTP future submission boundary, not socket transmission or exchange ACK.
