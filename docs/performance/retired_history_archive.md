# Thirty-minute hot retention for retired private replay proofs

Account checkpoints previously retained up to 100,000 retired trade proofs for
90 days and rebuilt their ownership routes on every restart. maker02's copied
checkpoints contained 9,013 / 19,535 / 80,929 such rows. Most describe settled,
zero-inventory tokens with no surviving order root. They do not need to remain
in every hot account copy.

## Policy and scope

`HOT_RETENTION_MS` is 30 minutes. Age starts at creation of the retirement proof
(`retired_at_ms`), or cold restoration of an archived proof; an order-audit proof
uses `audited_at_ms`. Startup migrates eligible rows once, and the existing cold
account owner checks every five seconds, retiring at most 128 rows per turn.
The interval is an eligibility threshold: busy queues, failed storage, and
unresolved dependencies retain the hot proof rather than losing it.

Trade proofs require a terminal status, known settlement, exactly zero physical
and instance inventory/order/maintenance reservations, no parent order/OID
mapping, no live trade, and no unresolved trade/anomaly. Nonzero dust is protected.
Zero-fill retired order-audit proofs require no surviving order or anomaly.
A surviving parent order can still depend on a trade proof for filled-quantity
validation and cannot be separated from that proof by this cleanup.

This change archives **retired proofs**, not arbitrary terminal orders or all
zero-position entries. Legacy zero-position keys are still used to prove the
unique owner of an unknown historical trade. Removing those requires a separate
cold ownership projection and message-based recovery path. Historical order
roots likewise require a complete terminal audit and joint retirement. The
existing settled-event GC remains responsible for those live lifecycle rows;
neither age nor a zero economic balance alone authorizes their deletion.

## Ownership, durability, and recovery

Each account's `<ledger>.history.sqlite3` holds exact, checksummed proofs. A
FULL-synchronous SQLite transaction commits proofs and the advisory membership
filter before any hot removal enters the account WAL. A crash between commits
leaves duplicate evidence, not missing evidence. Startup validates account
identity, archive generation, filter checksum, and SQLite integrity; missing or
older required archives fail closed. Repeated archival is idempotent and
conflicting immutable proofs cannot overwrite the original.

The hot ledger is schema v2. It accepts/migrates v1 checkpoints; old SDK binaries
reject v2, preventing a downgrade from silently ignoring archived identities.
The archive must be backed up and restored together with its ledger/WAL. An old
binary cannot be used directly against an archived v2 ledger. Restoring an old
pre-deployment account snapshot after new trading would discard new economics
and is not an acceptable rollback procedure.

The existing cold account thread owns SQLite connections, candidate selection,
and hydration. File I/O occurs outside account control/state locks. Hot removal
rechecks exact proof identity and eligibility after the disk commit and defers
while the lifecycle mirror has an unapplied watermark. It changes no balances,
reservations, economic compacted totals, or strategy-local state.

Private owners read a fixed 512 KiB append-only atomic membership filter per
account. It is advisory and has no authority to apply or suppress a fill. No
shared live account read, allocation, lock, file I/O, or old buffer destruction
is added by that lookup. The cold owner publishes bits only after durable commit.
The cold reader has a 512 KiB SQLite page-cache limit and does not map the whole
archive. Index lookup explicitly uses `proof_base`, avoiding SQLite's otherwise
observed scan of all rows sharing `kind`. Each lookup returns at most 128 legs.

An archive candidate uses the existing private repair credit and a new capacity-
one command lane to the **existing** cold account worker. The original owned
batch and completion return over the existing capacity-one repair reply lane.
Other live private traffic retains priority. The private owner reroutes the
returned batch through normal validation and deduplication. This also handles
filter false positives: a genuinely new fill still gets exactly one strategy
delivery before cold accounting. A cache hit cannot become permission to book
old economics. Queue/storage/validation failures keep replay incomplete, request
recovery, and never report a successful completion. No new thread or CPU role is
introduced; CPU placement remains unchanged.

## Validation

- Account library: 355 passed, 23 ignored (including archive TTL boundary,
  backward clock, dust/reservation protection, crash ordering, corruption,
  missing/wrong-account archive, immutable conflict, restart, and duplicate
  replay tests).
- Polymarket suite before the final storage hardening: 465 passed, 27 ignored.
- Final private archive integration: 3 passed, covering a real archived maker
  proof through the cold worker, a filter false positive/new fill, and failed
  completion/repair credit recovery. No live venue requests are made.
- Copied maker02 checkpoint **and WAL** migrations preserve validated economic
  roots and survive a second open without loading archived payloads into hot
  maps. Local debug runs archived approximately 101,560 proofs across the three
  accounts, leaving approximately 8,045 inventory/reservation-dependent proofs.
  Counts depend on the exact 30-minute cutoff; they are not a live inventory
  statement.

The ignored `maker02_archive_migration_and_lookup_benchmark` reads only a copied
fixture directory selected with `HEXBOT_ARCHIVE_FIXTURES`; it creates and removes
its own temporary ledgers. It reports N/P50/P99/P999/max separately for advisory
filter lookup and bounded cold disk lookup, plus migrated/hot counts and
first/second-open durations. The disk benchmark excludes first connection open
and message queueing. Live end-to-end/private/registration percentiles and memory
must be measured independently after rollout; isolated lookup timings are not a
substitute for those measurements.

### Linux release evidence (maker02, CPU 0/1, nice 19)

Final account library: **355 passed, 23 ignored**. The same copied checkpoint/WAL
prefixes were SHA-256 verified before the isolated migration; no live ledger was
opened or mutated by the benchmark. Archive rows: 8,636 / 18,073 / 74,856;
remaining trade proofs: 416 / 1,509 / 6,115 (hex001 / zhu03 / zhu02).

| Boundary | Account | N | P50 µs | P99 µs | P999 µs | Max µs |
|---|---|---:|---:|---:|---:|---:|
| Fixed filter lookup | hex001 | 10000 | 0.079 | 0.080 | 0.083 | 0.571 |
| Fixed filter lookup | zhu03 | 10000 | 0.079 | 0.081 | 0.081 | 0.375 |
| Fixed filter lookup | zhu02 | 10000 | 0.080 | 0.081 | 0.083 | 8.615 |
| Cached cold disk lookup + checksum/decode | hex001 | 100 | 12.781 | 27.299 | 43.792 | 43.792 |
| Cached cold disk lookup + checksum/decode | zhu03 | 100 | 12.872 | 28.333 | 51.079 | 51.079 |
| Cached cold disk lookup + checksum/decode | zhu02 | 100 | 13.067 | 20.564 | 48.965 | 48.965 |

Filter queue depth/overflow 0; disk in-flight 1, ending depth/overflow 0. These
are empirical nearest-rank quantiles; with 100 disk samples P999 equals maximum.
Message waiting and first connection open are excluded. The zhu02 filter maximum
is retained in the report despite its much smaller ordinary quantiles.

First open (including WAL recovery and initial archival) took 975 / 1,492 / 4,932
ms; second open took 168 / 258 / 830 ms. The second open also has an empty WAL,
so this is not a controlled archive-only speedup claim. Serialized hot states
are 9,449,783 / 14,532,654 / 40,990,009 bytes, not RSS measurements.

Raw evidence is retained locally in hexbot's
`docs/evidence/maker02_startup_seed_20260928T0810Z/` with the exact source and
fixture manifests. Live deployment results are recorded in hexbot's startup
seed report after the observation window completes.
