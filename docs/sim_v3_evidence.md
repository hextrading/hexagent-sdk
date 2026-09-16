# Offline simulator evidence profiles

This is an opt-in offline SDK patch. Existing live SDK pins and parser/worker
routing are unchanged. `legacy_age` is the default and preserves v2 economics.

## Configuration

`[backtest]` (use the existing section name in the consuming config):

```toml
sim_v2_raw_server_clock = true
sim_v2_strict_admission = true
sim_v2_admission_audit = true
sim_v2_admission_unknown_reject = true
sim_v2_book_continuity_mode = "snapshot_hold_estimated"
sim_v2_book_continuity_evidence_path = ""
```

Modes:

| Mode | Book eligibility | Claim |
| --- | --- | --- |
| `legacy_age` | Existing source-age gate | Age heuristic; v2 control |
| `snapshot_hold_estimated` | Last applied normalized book until explicit gap/reconnect or replacement | Historical sensitivity assumption |
| `require_verified` | Exact selected book has an archive certificate covering decision time | Retrospective archive completeness, **not** strategy-time knowledge |

The two experimental modes require strict admission and raw server clocks.
Absent eligible coverage aborts with a clear `simulator-data-unknown` error;
`sim_v2_admission_unknown_reject=true` instead runs the explicit conservative
unknown-rejection scenario. Unknown is never a venue post-only crossing label.
The same selected policy gates post-only/taker admission, resting trade fills,
book-through and depletion matching. Client book visibility clocks remain
separate and are never advanced by server evidence. Following a gap, stale
orders rebase on the next eligible book before inferred queue progress resumes.

Admission rows expose `covered`, `eligible`, `evidence_confidence`,
`coverage_scope`, `book_continuity_mode`, `continuity`, `source_age_stale`, and
`arrival_evidence`. Coverage categories include existing `known`,
`missing_server_book`, `stale_server_book`, plus `estimated_snapshot_hold`,
`continuity_unproven`, `continuity_gap`, `continuity_reconnect`, and
`continuity_owner_mismatch`. Keep known / estimated / unknown separate.
`coverage_scope` is `retrospective_archive_coverage` only for a covering
certificate; otherwise `recorded_snapshot_assumption`, `source_age_heuristic`,
or `unproven`. An old book may be eligible under an explicit assumption while
`source_age_stale=true`; this does not assert that a 2-second-old book is verified.

## Book continuity journal and certificates

`BookContinuityReplay::from_path(path, expected_iid)` validates the journal at
startup; `Simulator::set_book_continuity_replay` installs it before replay.
Use `--book-continuity-evidence PATH` with the fixed-command example, or the
config path above with full strategy backtests. Exactly one owner is required;
there is no fallback to another instance's records.

Each JSONL row is `BookContinuityRecord`: `iid`, `token`, `event_epoch`,
`evidence_id`, `kind`, `session_id`, `sequence`, `previous_sequence`,
`source_ns`, `receive_ns`, `apply_ns`, `book_source_ns`, `book_receive_ns`, and
`provenance`. Optional values must be represented explicitly as null when
absent. `kind` is `snapshot`, `sequenced_update`, `sequence_heartbeat`, `gap`,
or `reconnect`. `evidence_id` is a strictly increasing per-token recorder
identity, **not a venue sequence**. Exact duplicate rows are idempotent;
conflicting duplicate IDs and decreasing nonduplicate IDs fail at startup.
Rows must have nondecreasing apply times; explicit source times are positive,
not later than apply time, and cannot regress within a token's archive.
Client-only gap/reconnect rows have no source time and apply at their recorded
receive time. This is a client observation boundary, not measured venue time.

A snapshot anchors a session. An update must explicitly link its previous
sequence to that session's last sequence and advance it. A sequence-aware
heartbeat must explicitly report that same book sequence. An ordinary message,
trade, timer, replay row number, quote sequence, or generic heartbeat never
proves continuity. Gap/reconnect breaks a chain; only a new full snapshot can
re-anchor verified coverage. A late gap from an older session cannot invalidate
an already anchored new session. Delayed proof for a superseded book does not
invalidate its newer replacement.

At startup, validated chains produce immutable **metadata-only** certificates:
`[book_source_ns, next_linked_update_or_sequence_heartbeat_source_ns)`. The right
endpoint is exclusive. A terminal chain has no extrapolated future endpoint.
Broken links do not bridge missing data. A heartbeat can extend coverage only
through its explicit source watermark. Certificates bind owner, event, token,
session, raw book source and recorded receive identity. Equal timestamps remain
legal book events; repeated indistinguishable source/receive identity cannot
become verified. No future BBO, depth, trade price, or order outcome enters a
certificate. A certificate is copied into state when its matching book is
actually captured, before matching runs. It cannot install that book early.

This is explicitly **retrospective archive coverage**: future protocol metadata
may certify a past interval, but the strategy could not have known that proof at
the earlier time. The separate causal `verified_through_ns` state is only a
watermark diagnostic. With delayed observations it would by itself usually
cover no intervening arrival times, so it is not used as a perpetual admission
certificate. Full books still enter only through causal server-feed application;
source time must not exceed the applied decision time. Same-time gap/reconnect
invalidation runs before feed and request work. Positive observations run after
their matching feed rows; certificate binding is atomic with book capture.

All new mutable state belongs to the simulator's single offline owner.
Protocol/evidence parsing and archive allocation happen before replay, bounded
by 262144 JSONL rows and 16384 bytes per row. Existing server DES owns scheduled
observations; no cross-thread lane or new worker is added. Active token state is
bounded at 128 and retires with events; exhaustion/routing errors fail closed.
Admission rows use the existing 8192-row preallocated queue, drained by the
offline loop outside quote callbacks; overflow is fatal and counted. Immutable
archive lookup is a per-token binary search at book capture; subsequent
eligibility checks copy fixed-size state. New updates add no locks or I/O.
Token registration retains bounded existing string ownership; no new per-update
string allocation is required by continuity lookup/state mutation.

## Order arrival evidence

The fixed-command adapter accepts `--arrival-evidence PATH`. It validates the
complete PLACE population before replay: unique coid and exact iid/token/event
ID/epoch/attempt/dispatch/completed binding. Full strategy-generated coids do
not consume this table. Existing split point and scheduler-floor behavior are
unchanged; every actual selected point remains `selected_kind=modeled_split`.

`ArrivalEvidenceRecord` fields: `iid`, `coid`, `token`, `event_epoch`, `epoch`,
`event_id`, `command_kind` (`place`), `attempt_id`, `dispatched_ns`,
`completed_ns`, `kind`, `lower_ns`, `upper_ns`, `clock_domain`,
`clock_offset_lower_ns`, `clock_offset_upper_ns`, `provenance`.
For `measured_client_interval`, lower equals dispatch and any finite upper equals
completed; HTTP timeout has no upper bound. These are measured **client stage**
endpoints, not wire-send/venue-ingress measurements. Unknown clock offsets remain
null, so local bounds must not be promoted into verified exchange-clock bounds.
`measured_exchange_point` requires an explicit positive point in exchange domain;
none is present in the available records. Evidence annotations never change
matching time, choose a latency parameter, or use accepted/rejected/fill labels.

## Actual historical and future collector limits

The BTC normalized parquet records contain depth and raw/local clocks, but no
wire snapshot/delta kind, venue sequence/previous sequence, session, or
book-hash/sequence-aware heartbeat. Their depth is truncated by the existing
recorder (top 5). They cannot yield verified continuity retrospectively. The
frozen historical continuity journal is therefore empty. `require_verified`
is useful here only as a fail-closed diagnostic smoke test. Snapshot hold is a
separately reported assumption. Existing order audit supports client-stage
intervals, not true exchange ingress. Quote sequence is a strategy sample ID.

A future collector DTO, `recorder::BookProtocolRecord`, preserves wire message
kind, venue sequence/previous sequence, connection/session identity, optional
venue book hash, source/local clocks and a separately named recorder sequence.
`MarketRecorder::record_book_protocol` is an executable recorder-worker entry
point. It appends `book_protocol_evidence` rows using existing buffers/flush
lifecycle and rejects missing/wrong token/event routes. Absent venue source time
remains null in JSON and zero only in the required numeric parquet column.
It never substitutes client time for venue source time.

**The current live parser/MarketEvent lane does not call this new API.** The
normal live SDK pin is unchanged. Deployable future collection still requires:
(1) extract actual fields before normalization; (2) bind connection/session and
owner identity at subscription/connection setup; (3) send a bounded compact
message to the existing recorder worker, with explicit gap/overflow recovery;
(4) integrate and measure that SDK change in live builds. There is no safe way
to reconstruct those missing fields from today's normalized `MarketEvent`.
The new DTO is for the background worker; its Strings must not be allocated or
serialized in a quote callback. No new thread/topology is introduced here.

## Validation and focused benchmarks

Run serially from the SDK root (the coordinating root agent owns Cargo):

```sh
cargo test --offline -p hexagent-exchange --lib exchange::sim_v2 -- --test-threads=1
cargo test --offline -p hexagent-exchange --lib recorder::protocol -- --test-threads=1
cargo test --offline -p hexagent-exchange --lib recorder::writer::tests::book_protocol -- --test-threads=1
cargo check --offline -p hexagent-engine
cargo check --offline -p hexagent-exchange --example replay_audit_commands
cargo test --offline --release -p hexagent-exchange --lib exchange::sim_v2::evidence::tests::continuity_state_benchmark -- --ignored --exact --nocapture --test-threads=1
cargo test --offline --release -p hexagent-exchange --lib exchange::sim_v2::evidence::tests::archive_lookup_benchmark -- --ignored --exact --nocapture --test-threads=1
cargo test --offline --release -p hexagent-exchange --lib exchange::sim_v2::exchange::tests::continuity_book_capture_benchmark -- --ignored --exact --nocapture --test-threads=1
```

Each benchmark emits 100000 events, median/P99/P999/max, capacity, queue depth
and overflow. `continuity_state_benchmark` measures observation plus eligibility
logic (causal watermark diagnostic in its verified branch);
`archive_lookup_benchmark` measures immutable identity lookup plus interval
check over 4096 certificates; `continuity_book_capture_benchmark` measures book
application through return, including bounded evidence capture, with no resting
orders. They are focused offline measurements, not live end-to-end latency
claims. Setup, file parsing, sorting/export and timing-buffer allocation occur
outside measured sections. Actual performance numbers belong to the coordinating
run's results; this document does not claim unrun tests or benchmarks passed.
