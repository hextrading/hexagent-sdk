# Execution publication tail follow-up, 2026-09-27

The previous rollout did not pass overall registration latency acceptance:
median minute P99 rose from 584 to 983 microseconds despite the ledger phase
improving. A fresh fixed window, 14:41–15:11 UTC, has N=3,880 registrations
over 28 nonempty minute exports: total P50/P99/P999/max 53.2/601.25/734.3/1790
microseconds, ledger 32.8/55.25/185.5/1100 and publication
20.5/492.45/668.8/1750. These are medians of minute quantiles, not pooled
event quantiles. Original logs and data/record exports are retained in hexbot
under `docs/evidence/maker02_publish_tail_20260927T1511Z/`.

Read-only hardware-cycle sampling of the three private owners captured 7,529
samples with no loss. 1,242 stacks include ExecutionStateOwner::apply; 732 of
those include ArcSwap debt payment. Other publication stacks include mimalloc
page/heap collection. This establishes costs worth investigating, not the
cause of every wall-time outlier. Software cpu-clock sampling was unsupported.
The profiled interval is separate from the fixed pre-change latency window.

## Change and ownership

ExecutionReadMap leaves now contain shared immutable key/value rows. Updating
one row copies the outer/shard pointers and the affected leaf's Arc pointers,
without allocating copies of all historical strings and TrackedOrder values
in that leaf. Hash/equality/borrow all use the same string key; replacement
replaces the complete row so a rebind cannot retain its prior value. Old
snapshots keep their own immutable rows. Reader interfaces, coherent three-map
publication, private routing, retirement certificates and persistent formats
are unchanged. Point operations also reuse their partition hash.

Only the existing private owner mutates the execution maps. No new worker,
lock, global map, authority or message lane is added. Existing lifecycle FIFO
capacity/ordering/fail-closed overflow and private-event priority are retained.
The quote path is unchanged. Leaf/container and new-row allocations still
exist on this background owner; this is an incremental reduction, not a claim
of allocation-free publication. ArcSwap's global reader-debt scan remains.

Eleven prewarmed observations use the existing 65,536-entry producer FIFO:
load/clone, first identity index, coid snapshot, reverse index (including any
old-OID removal), oid snapshot, token index, token snapshot, ArcSwap exchange,
old snapshot release, CPU and estimated off-CPU. Empty-token updates report
zero token-index duration. Phase clocks stop before observations are enqueued;
the existing enclosing publication/total registration boundaries include all
observer overhead. CPU covers load through release, plus clock overhead;
off-CPU is wall minus thread CPU, not a lock-specific measurement. Unsupported
CPU clocks omit those two samples. Drop-new overflow is counted and advisory;
the existing CPU-affined latency-dump worker drains and aggregates the FIFO.

## Focused evidence

macOS release, LTO off, 16 codegen units, default SDK test allocator. Same-process
alternating old/new publication of all three maps; 45,000 retained identities,
5,000 additional inserts. Command input preparation is outside timing;
construction, pointer replacement and old map destruction are inside. This
microbenchmark excludes owner mutable maps, ArcSwap and telemetry, and cannot
substitute for the live total registration acceptance. Queue depth/overflow
are zero for this synchronous fixture.

| Boundary | N | P50 ns | P99 ns | P999 ns | Max ns |
|---|---:|---:|---:|---:|---:|
| Owned-string rows, publish/drop | 5,000 | 28,677 | 68,567 | 82,931 | 105,494 |
| Shared rows, publish/drop | 5,000 | 8,929 | 14,769 | 26,053 | 47,346 |
| Owned-string lookup | 5,000 | 121 | 175 | 208 | 2,238 |
| Shared-row lookup | 5,000 | 120 | 174 | 205 | 21,066 |

Lookup maximum regressed in this run; its P99/P999 did not. Live end-to-end,
reader, private-application and queue measurements remain required.

Release suites: exchange 931 passed, 44 ignored; engine 147 passed, 7 ignored.
The added regression verifies zero historical value clones, old-reader
immutability, replacement, duplicate removal, replay and independent branches.
Existing suites exercise lifecycle ordering, queue overflow, ownership
isolation and reconnect/recovery. A fee fixture exposed an asynchronous setup
race (929 passed/1 failed initially; isolated rerun passed). It now waits for
its already-existing test-only owner barrier before injecting the fill; no
production synchronous request was added.

Live rollout and overall acceptance results are recorded in hexbot. Do not
treat this isolated publication improvement as proof that the overall tail
has been resolved.
