# Live price-only CLOB

Live feeds now publish the BBO advertised in `price_change` and
`best_bid_ask` immediately. They do not maintain incremental depth, compare
the advertised BBO against reconstructed depth, request consistency repairs,
or wait for a quiet period. A `book` snapshot supplies only its best prices;
its levels and quantities are discarded after parsing. Paper and standalone
record mode keep the existing full-depth path.

Each subscribed condition has one CLOB-owner scalar price state and one
exchange timestamp shared by both message kinds and both outcome tokens.
Older exchange timestamps are rejected; equal timestamps follow wire order.
For Down, Up bid = 1 - Down ask and Up ask = 1 - Down bid. Invalid, missing,
unknown-token and implausibly future timestamps cannot advance the cursor.
The original receive clock is retained only as transport provenance for
telemetry and prewarmed subscription handoff; it never orders prices.

Direct messages with missing BBO fields are ignored. Explicit venue 0/1
boundaries clear the corresponding tradeable side. The existing QuoteTick
transport represents this as bid=0 or ask=1; live consumers must map these
sentinels to absent prices, not retain an older BBO. Quantities are always
zero/unknown and are not live trading inputs. Both fields are replaced
together, so a new price is never paired with an older opposite side.

One price checkpoint from either outcome readies the condition. Prewarmed
subscription activation transfers cached prices and health immediately,
without waiting for another message. The original server and receive clocks
survive that handoff. Tick metadata is forwarded before activation prices.
Only logical subscription prices are forwarded. Disconnect and private-event
recovery/admission remain independent of the absence of an L2 book.

The existing pinned CLOB owner exclusively mutates its price map; the strategy
owner exclusively mutates its scalar L1. Startup creates all feed map keys.
There are no new workers, locks, global mutable maps or synchronous calls from
the strategy to the feed. Quotes use the existing bounded replaceable lane
(8,192 slots); its overflow evicts old replaceable observations. The separate
2,048-slot ordered public-event lane and lossless private lifecycle lanes are
unchanged. Up/Down siblings coalesce only within an already received frame.

The existing owned `QuoteTick.symbol` still requires one string allocation at
the feed-to-queue boundary. This occurs before strategy quote processing;
depth vectors, per-level trees and per-update BBO wait records are eliminated
in live mode. Migrating the public event ABI to startup-interned symbol IDs
is separate work; this change does not add another shared allocation scheme.
The focused benchmark counts the remaining allocations explicitly.

New prewarmed observation stages measure each raw wire kind's server timestamp
to receive age, live BBO application, and (in the hexbot consumer) receive to
strategy L1 application. The wall-clock source ages include venue distribution,
network and local receipt scheduling and are not pure network latency.
Observations use the existing bounded 65,536-entry queue and are binned by the
background latency worker; telemetry overflow cannot block market/private data.

Tests cover snapshot-free initialization, Up/Down inversion, mixed-source
ordering, equal-timestamp replacement, duplicate replay, invalid/future data,
empty-side clearing, subscription/instance isolation and prewarmed handoff.
Existing bounded-queue overflow and private-lifecycle suites remain required.
`live_bbo_parse_publish_benchmark` compares the retained old full-depth path
and direct BBO path on the same frames and reports N, P50/P99/P999/max,
allocations and queue boundaries. Its direct-call business queue depth is zero;
real queue/backpressure validation is a separate 45-minute maker02 observation.
