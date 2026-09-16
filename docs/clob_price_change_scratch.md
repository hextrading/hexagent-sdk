# Bounded price-change scratch on the CLOB owner

A maker02 frame at 2026-09-16 13:29:49.632 UTC took 27,849 µs in
`ClobLocalBooks::apply_price_change`. The 606-byte frame contained two level
deletions. Four consecutive samples in the existing perf recording spanned
19.634 ms in `apply_price_change -> malloc -> mimalloc arena collect/purge ->
madvise -> kernel PTE removal`. The allocator call's copy/top/hash instruction
sequence matches `before.insert(token.to_string(), book.top())`. This identifies
a synchronous allocation slow path; it does not identify which earlier frees
accumulated the purge work or prove every microsecond was allocator work.

The ordinary price-change path now borrows token identities from the decoded
frame and stores per-token counts, first BBO, final reported BBO, and off-tick
flags in a stack-backed `ArrayVec`. Validation order, touched-token order, and
condition deduplication also use bounded scratch. At startup, each exact condition
string receives an owner-local Copy `u16` identity stored in the existing role.
The startup-only interning map is then dropped. Duplicate conditions reuse the
same identity regardless of token aliases; shared tokens do not merge distinct
conditions. Unknown roles or exhausted `u16` identities fall back to exact
borrowed-key comparisons, never a shared sentinel. This avoids repeatedly
looking up role strings inside the bounded condition-dedup loop. The decoded input already has
at most 64 changes, so distinct tokens and all derivative lists have at most 64
entries. No delta is truncated. A 65-entry frame remains rejected by the existing
decoder before book mutation. Lookup is bounded linear search; the benchmark
includes 64 distinct tokens as well as the common two-token frame.

This scratch is owned by the existing CLOB worker and expires before the decoded
frame. There are no new shared fields, queues, locks, workers, affinity roles,
I/O, or deferred critical book mutations. Per-entry wire sequence, first-before
comparison, lexical BBO validation, field merge/overwrite behavior, per-condition
health reconciliation, and stale/unseeded/repair handling retain their prior
semantics. Subscription routing and event queue/backpressure behavior do not
change.

The scope is temporary scratch. Persistent pending-BBO keys, BTreeMap level
insertions, owned output snapshots, and exceptional diagnostic formatting retain
existing allocation behavior. The actual captured frame falls from 31
allocations / 3,898 bytes to 9 / 1,748 bytes at the complete apply boundary.
This is not an allocation-free CLOB path and is not a guarantee that mimalloc
purge cannot recur. The migration path for remaining allocations is resident
per-token BBO state and reusable owned output buffers with explicit transfer and
return semantics; global purge settings are unchanged.

Validation uses a test-only copy of the exact previous production function from
SDK `acf28b5399ffa12308220e74ba42b87408688ed0`, compared against the real new
method. Tests cover the captured deletions, replay/stale updates, interleaved
repeated tokens, missing/overwritten BBO fields, invalid/off-tick mixed entries,
recovery and unseeded reconnects, owner isolation, and 64/65 boundaries. All
market-module tests run serially with the existing allocation counter.

The ignored benchmark measures already-decoded fields through actual apply
return: two captured deletions (100,000 samples per version) and 64 distinct
deletions (10,000 per version), reporting P50/P99/P999/max and total allocation
counts/bytes. Parser, fixture restoration, output drop, queues, network, and
strategy processing are outside this boundary. The local development test binary
uses its counting System allocator, not the production mimalloc allocator;
allocation-counter overhead is included. Reported timings are a local
before/after comparison, not production end-to-end percentiles or a reproduction
of the allocator purge. Queue depth/overflow are not applicable to the direct
same-thread microbenchmark and require separate live observation.

Final local validation: 11 focused tests passed, and the full market module
passed 96 tests with 9 ignored (the focused tests are included, not additive).
The same frozen binary's ignored benchmark passed separately. Declared temporary
type storage is 9,248 bytes: token scratch entry 112 bytes, scratch array 7,176,
validation order 520, touched order 1,032, and reconciled indices 520. This is not
a measured compiler stack-frame size and excludes existing decoded/output state.

Final benchmark values, in microseconds:

| Input | Version | N | P50 | P99 | P999 | Maximum |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Captured two deletions | Previous | 100,000 | 79.354 | 157.633 | 229.854 | 386.731 |
| Captured two deletions | Scratch + condition IDs | 100,000 | 55.225 | 121.373 | 318.646 | 11,123.083 |
| 64 distinct deletions | Previous | 10,000 | 1,376.152 | 1,727.391 | 2,408.455 | 3,036.674 |
| 64 distinct deletions | Scratch + condition IDs | 10,000 | 1,083.618 | 1,432.676 | 2,039.957 | 2,449.350 |

The 64-token median regression in the initial scratch-only version is removed.
The final two-token run improves P50/P99 but has a worse P999 and an 11.123 ms
maximum. The microbenchmark does not attribute that outlier or justify claiming
all tails improved. Production end-to-end tail and queue observation remains a
separate acceptance boundary; remaining allocations can still enter slow paths.


One additional diagnostic alternated old/new actual apply for 100,000 samples
per version, also alternating which version ran first in each pair. Thread CPU
clock reads (`CLOCK_THREAD_CPUTIME_ID`, available with reported 1 ns resolution)
were outside the unchanged wall boundary. The CPU interval is slightly wider
than the wall interval. All samples over 1 ms were retained: 11 old / 4 new,
15 stored / 0 omitted (bounded storage capacity 1,024).

| Version | Clock | N | P50 µs | P99 µs | P999 µs | Maximum µs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Previous | Wall | 100,000 | 99.228 | 169.442 | 384.446 | 11,420.334 |
| Scratch + condition IDs | Wall | 100,000 | 70.382 | 120.447 | 290.829 | 11,211.268 |
| Previous | Thread CPU | 100,000 | 100.338 | 170.828 | 363.102 | 1,095.964 |
| Scratch + condition IDs | Thread CPU | 100,000 | 71.416 | 121.967 | 280.877 | 921.361 |

Both versions reproduce about 11 ms wall tails in the same narrow paired-run
region, with approximately 0.5–1.1 ms thread CPU and about 10 ms unaccounted as
thread CPU. This is evidence of substantial off-CPU waiting/preemption in this
local diagnostic, not 11 ms of apply computation. It does not distinguish
blocking from scheduler preemption, retroactively prove the earlier single
11.123 ms sample's cause, or replace production perf evidence/observation. The
original unfavorable P999/maximum results remain above; this was one diagnostic
run, not repeated selection of favorable outcomes. Production source remained
unchanged during this test-only addition.
