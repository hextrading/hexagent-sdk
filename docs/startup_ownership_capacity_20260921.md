# Startup ownership capacity recovery

The first maker02 restart of hexbot 72dc48a / SDK 1d4f234 on 2026-09-21
16:30:49 UTC stopped before feeds/strategies started: zhu03 could not rebuild its
private-order ownership index. The durable ledger contained 53,295 terminal
orders (52,117 cancelled, 969 filled, 209 rejected), occupying 81.3% of the
65,536-slot table. Its 128-probe insertion bound was exhausted. The constants
predate this rollout (commit c1677ce5); no ledger entries were deleted or altered
to bypass the failure. zhu02 retained 26,251 terminal routes.

Size the fixed table once, before runtime publication, to the next power of two
of four times the recovered valid route count (minimum 65,536; maximum 4,194,304
slots). Reject count overflow or excess capacity before allocation. Hash and
probe against that table's capacity. Keep the 128-probe budget, all historical
terminal mappings, immutable per-entry ownership, and existing explicit runtime
insertion failure behavior. Publish recovered count/capacity at startup only.
This allocates 262,144 slots for zhu03 and 131,072 for zhu02. There is no runtime
resize or new lock, channel, worker, queue, or quote-path operation.

Local replay uses all 79,546 sanitized production order IDs with exact client and
instance identity assertions. The old zhu03 table rejects 7 inserts; the new
implementation admits all 53,295, with zero overflow on either account. Existing
ordering, retirement/replay and isolation tests remain; new tests recover 80,000
terminal orders, check case-normalized lookup, replay/removal, instance isolation,
startup capacity bounds and saturation without overwriting existing routes.

The shared ownership publication mechanism is an existing execution/private lane
component; startup owns the table allocation, and existing workers publish entries
using the unchanged ArcSwap protocol. A future account-local owner publication
migration remains separate. Larger tables increase cold bulk-retirement scan work;
this patch does not run that scan in the quote path. Runtime capacity remains
finite and exhausted probes fail closed instead of dropping history. The 25%
startup target provides headroom, not an unlimited-lifetime retention policy.

Evidence: [focused tests](evidence/startup_ownership_20260921/focused-tests.txt),
[live identity replay](evidence/startup_ownership_20260921/live-replay-debug.txt).
Timing in the latter is debug-only and not a production end-to-end claim.

Exchange full suite: 905 passed, 36 ignored, 0 failed.
