# Raw Polymarket protocol capture

This opt-in lane connects the real CLOB WebSocket reader to the **existing**
recorder worker. The original live SDK pin and deployment are unchanged.

```toml
[recording]
book_protocol_evidence = true
```

The default is `false`. LIVE, PAPER and RECORD can capture; backtests do not
enable a live collector. The general `ExchangeMarket` and `MarketEvent` payloads
do not change, and no strategy/account state is shared with the collector.

## Artifacts and authority

Under the existing recording output directory:

- `book_protocol/<capture_id>.frames.jsonl`: the exact text frame copied before
  the in-place JSON parser mutates its buffer. JSON escaping is transport only:
  decoding `payload_utf8` restores the original string, including number/string
  representations, whitespace and unrecognized fields.
- `<capture_id>.records.jsonl`: bounded parsed protocol observations written
  through `MarketRecorder::record_book_protocol` by that same worker.
- `<capture_id>.manifest.json`: initial `archive_complete=false`, periodically
  updated counters, and a closing completeness assessment. A crash, missing
  footer, write failure, pool exhaustion, oversize frame, conflicting route or
  missing token route must not be treated as a complete archive.

All rows have **public-feed** authority. The compatibility field `iid` is
`feed:polymarket`, and `owner_scope=public_feed`; it does not mean `btc01` or any
other strategy. `validate_strategy_owner` rejects these rows. An offline
conversion needs an independently frozen subscription/instance binding and
must preserve this original authority in its provenance.

`lane_role` records `active`, `candidate` or `standby` at text receipt.
`delivery_claim=received_only_not_strategy_applied` applies to all of them.
Candidate seed messages and drain-only standby messages were not necessarily
delivered to a strategy. They must never be merged into its historical feed
merely because they exist in this archive. Protocol rows join raw frames by
capture, connection/session and local `frame_sequence`.

The selected feed may suppress stale books, canonicalize two token books,
coalesce multiple updates in one frame or lose a replaceable downstream
snapshot. Raw receipt alone does not prove the resulting book was applied to
a particular strategy. Future exact input reconstruction also needs an
owner-side application trace.

## Fields that are measured

The parser extracts `book` as snapshot and individual `price_changes` entries
as deltas in original order. It retains actual `hash`, `sequence` and
`previous_sequence` fields when present. Absent fields remain `None`.
The original numeric timestamp and its normalized source ns are separate from
client receive ns. Missing source time is not replaced with client time.
Raw frame text remains available to inspect all fields and future schema
changes, including sequence fields this bounded extractor does not recognize.

Connection/session IDs and recorder/frame sequences are **local observation
identities**, never venue sequence proof. A hash alone does not establish a
gap-free sequence chain. Ordinary text PING/PONG is retained only as raw text;
no generic heartbeat becomes `SequenceHeartbeat`. The archive currently makes
`continuity_claim=none_raw_observations_only`, including when complete.

Token/event routes are created from subscribed Gamma/instrument identities.
An explicit ten-digit event slug suffix supplies `event_epoch`; static markets
without one use zero as documented unknown, with their condition ID retained.
Neither receipt/source time nor an order-ID suffix assigns an event.

## Ownership, capacity and loss semantics

Each physical WS task owns one `BookProtocolSession`, with a fixed capacity of
128 token routes. Its connection identity survives standby promotion. A
promotion emits a gap because the selected local book was inherited from a
different connection. Socket closure/retirement emits a gap; session changes
never silently extend the old book's validity.

The capture-local shared transport consists of:

| Lane | Capacity | Producer / consumer | Overflow |
|---|---:|---|---|
| Protocol/raw messages | 8,192 | WS owners / one recorder worker | `try_send`, sticky loss count |
| Free raw buffers | 64 × 128 KiB | recorder returns / WS owners take | no replacement allocation; sticky loss and gap |
| Recorder wake | 1 | WS owners / recorder | coalesced notification; payload remains in the bounded message lane |

The raw pool is allocated before feeds start. Steady-state capture copies into
an existing buffer and queues a bounded value; it does not format, serialize,
allocate, block, acquire a mutex, perform file I/O or read an account. Metadata
borrows the existing simd-json tape and does not parse JSON again. Queue and
pool counters are capture-local atomics; all persistent state belongs to the
recorder worker. Serialization, batching, filesystem writes and buffer return
occur there. The worker retains its existing background affinity/topology;
private trade/lifecycle priority and routing are unchanged.

Oversize text, pool exhaustion and a full/disconnected message lane never
allocate a larger buffer or wait for disk. The manifest remains incomplete
after recovery. A gap may itself fail to enqueue when full; the independent
sticky counters still invalidate the entire archive. Raw and normalized
recording have separate completeness meanings: a complete raw text archive
does not certify complete normalized delivery or continuous venue BBO.

Shutdown drains on the recorder worker for at most five seconds while active
sessions close, then flushes both files and the manifest. Active producers,
pending messages or any recorded failure prevent `archive_complete=true`.
This is bounded evidence shutdown, not a new critical-lane wait.

## Validation and measurement

Focused tests are in `exchange/polymarket/book_protocol_capture_tests.rs`.
They exercise actual parser → bounded lane → existing recorder worker API,
byte-exact raw text, unknown fields/timestamps, duplicate observations,
session changes, public-versus-instance authority, conflicting token routes,
malformed frames and independent saturation of the queue and raw pool.

The ignored `benchmark_book_protocol_capture_100k` test runs 100,000 iterations
for both book and delta, with capture disabled/enabled. Its boundary is raw
copy + resident tape decoding + compact metadata enqueue; it reports frame
size, median/P99/P999/max, allocation counts and message/raw-pool high water
and overflow. Buffer recycling occurs after the timing sample; network,
book application, strategy work and recorder disk latency are excluded.
The benchmark cannot establish live end-to-end latency or disk throughput.
Numeric results belong to the root's serialized validation run.

## Remaining observations

This lane does not add exact order wire-dispatch, HTTP ACK or private-event
clocks. Existing latency/order audit observations remain separate. There is no
invention of a venue arrival point, clock offset, venue sequence, missing WS
history or historical model-ready event. Those require actual future capture
and a separately reviewed offline binding/conversion.
