# Publish validated CLOB data immediately

Continuous fine-grid CLOB updates could postpone publication because each
relevant update renewed a 50 ms quiet window while awaiting tick metadata.
Maker02's 12:32–13:17 UTC observation recorded a 221.86 ms maximum for
`clob_bbo_wait_deadline`, even though callback latency excluded that hold.

Valid seeded depth now publishes immediately when its local top matches the
advertised BBO. A valid standalone BBO quote also publishes immediately. Neither
waits for `tick_size_change`; only authoritative tick messages update order
precision. The `PendingQuote` buffer, tick-grid publication gate and renewable
`last_update_at` deadline are removed. Continuous updates cannot accumulate
publication debt behind a delayed tick event.

An inconsistent book still fails closed. Its repair deadline is fixed at
**first mismatch + 50 ms**, allowing sibling frames to complete the checkpoint.
A matching valid checkpoint releases immediately; expiry quarantines the book
and requests authoritative repair. Later frames/checkpoints never move this
deadline. Scheduler lateness is measured separately, so this is not a guarantee
that an unscheduled task runs at exactly 50 ms. Crossed/invalid/unseeded books,
stale messages, reconnect/replay, complementary-token mapping and condition
isolation retain their validation. A full valid snapshot with an empty side
still immediately clears that side; an incomplete standalone BBO is ignored.

Two separate policies remain: 250 ms coalescing for non-BBO quantity/depth
changes, and 500 ms healthy-state hysteresis before taker/requote eligibility
recovers. Neither is the removed tick/BBO publication quiet rule. Advertised
valid L1 quotes can still be forwarded while depth is incomplete; they do not
make the depth healthy or bypass strategy risk gates.

All modified state remains on the existing CLOB owner. No worker, affinity,
queue, cross-thread authority or locking is added. Removing grid inspection
also removes the quote float/string/Decimal conversions used only by that gate.
Existing adapter snapshot allocations and asynchronous repair handling remain;
a future bounded snapshot-storage migration is separate from this policy fix.

Regression tests cover immediate fine-grid books and quotes before tick metadata,
continuous changing tops, same/newer-timestamp mismatch deadlines, quantity-only
coalescing, confirmed empty sides and existing stale/invalid/replay/overflow
behavior. The old scratch-allocation reference test is adapted to the new
publication contract; it is no longer described as an exact historical policy
baseline.

The focused benchmark compiles the **same benchmark source** against original
SDK `c0f4e9834588c28dce5349fe722eef12a1c49dc3` in a detached worktree and the
new code. Each iteration seeds a book, then applies nine valid fine-grid BBO
changes 10 ms apart using virtual time (no sleeps). Report separately:

- Resident buffer copy + parse + apply elapsed wall time, N=9,000 after 100
  warmup iterations, excluding seeding, frame generation and sample storage.
- Virtual first valid update to first book publication, N=1,000. This isolates
  the policy hold and is not a measured live-network percentile.

Both executions are single-owner, with no transport queue (depth/overflow 0).
Local macOS CPU maxima include preemption and do not prove maker02 latency.
Validation and benchmark results follow. A new 45-minute live
window will separately check upstream CLOB holds, callback/quote queues, HTTP,
order status, private trades, positions and retained lifecycle ownership.

Original c0f4e983 release benchmark: resident frame copy/parse/apply N=9,000,
P50/P99/P999/max **4,652 / 23,403 / 29,990 / 47,638 ns**. Virtual first-valid
update to first publication N=1,000 is **130 / 130 / 130 / 130 ms**: the ninth
frame arrives 80 ms after the first, then renews another 50 ms. These policy
measurements reproduce the defect without real timer sleeps or network noise.

## Completed validation

`cargo test --locked -p hexagent-exchange --lib`: **949 passed, 47 ignored**, no failures. `cargo check --locked --workspace --all-targets` also passes. Existing deprecated/unused test warnings remain.

| Boundary | N each | Before P50 / P99 / P999 / max ns | After P50 / P99 / P999 / max ns |
|---|---:|---|---|
| resident_frame_copy_parse_apply_wall | 9,000 | 4,652 / 23,403 / 29,990 / 47,638 | 6,264 / 24,763 / 38,235 / 80,657 |
| virtual_first_valid_update_to_first_publication | 1,000 | 130,000,000 / 130,000,000 / 130,000,000 / 130,000,000 | 0 / 0 / 0 / 0 |

The artificial publication hold is **130 ms → 0** in this schedule. The new path publishes all nine valid price changes; the old path withholds them until its final deadline. CPU results therefore include the intended earlier snapshot work. No transport queue is used in either fixture; depth and overflow are zero. Live end-to-end conclusions require the subsequent maker02 observation.
