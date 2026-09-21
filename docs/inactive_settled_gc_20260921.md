# Give retained inactive instances a settled-history GC owner

During the maker02 observation on 2026-09-21, zhu03 still had 219–221 settled GC
candidates with one inflight condition. The persisted registry contains active
`btc02` and configured-but-disabled `eth02`; the restart snapshot retained 50,007
and 3,288 orders respectively. The coordinator requires a current certificate
from every persisted instance, but only enabled strategies installed a cold GC
mailbox. A missing eth02 consumer therefore blocks account-wide retirement,
contributing to the startup ownership table exhaustion fixed in PR #273.

The engine now sends the enabled instance set to the existing cold account owner
and waits for its startup acknowledgement before feeds/strategies can start. That
owner installs narrow GC capabilities for retained inactive instances and polls
one mailbox per existing 10 ms low-priority timer turn, round robin. Enabled
strategies retain their own GC owners. This does not erase the inactive registry,
redistribute its cash, change positions, or relax terminal/reservation/epoch
checks. Existing durable tombstones preserve late replay attribution.

The cold account thread alone owns the new optional capability vector, startup
partition and cursor. No new thread, affinity class, global mutable structure or
quote-path operation was added. The existing cold owner remains pinned through
`pin_private_account_cold`; its higher-priority lifecycle mirror and cold command
branches still precede the timer. GC's existing bounded lifecycle-owner request
and certificate protocol is reused; this patch does not expand its deletion
budget (8 orders + 8 trades per owner turn) or move it onto a strategy quote path.

Startup transfer uses the existing 4,096-command lane and a one-element reply.
Inactive owners are capped at 256 per account; each existing request mailbox has
capacity 2 and the shared certificate lane remains 1,024. Full/unbound startup
lanes or a missing acknowledgement fail startup closed. Repeating the same
configuration preserves queued generations; changing the partition or stealing
an already registered owner is rejected. Re-registration revokes an old mailbox
before it can delete anything. Full completion lanes retain certificates for
retry. Shutdown may abandon ephemeral GC progress; the durable candidate and
per-turn persistence support retry after restart.

Tests reproduce the disabled-owner stall, complete it without modifying balances
or an unrelated reservation, preserve a nonterminal order, reject stale owner
requests, cover round-robin bounds, idempotence, unknown membership, capacity,
unbound/full startup queue and ownership replacement. Existing reconnect/replay,
tombstone, stale-certificate and completion-backpressure tests are also included
in the full account/engine/exchange run: 328 + 134 + 905 passed, 57 ignored.

Evidence: [test summary](evidence/inactive_gc_20260921/test-summary.txt),
[read-only ledger registry](evidence/inactive_gc_20260921/ledger-owner-summary.txt).
Production improvement must be assessed from candidate/certificate progress,
private-event latency and queue overflow after deployment; a functional GC fix
is not evidence that upstream HTTP latency has improved.
