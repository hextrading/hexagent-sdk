# Private owner CPU and wall timing

`LifecycleTiming.private_owner` carries fixed, owner-written durations alongside
the original receipt. `PrivateOwnerSpan.cpu_ns` is a duration measured on the
calling thread; an unavailable or unmeasured CPU clock remains `None`. A CPU
timestamp is never transferred to another worker for subtraction. The current
worker's thread ID is distinct from the destination strategy's numeric owner.
Wall entry is read before CPU entry; CPU completion is read before wall
completion. Adjacent spans use independent paired marks: a start mark's wall
read cannot serve as the preceding span's completion. Raw validation checks
every measured CPU duration against its enclosing wall interval.

The stages cover these boundaries:

| Field | Boundary |
|---|---|
| `pre_route` | Current routing turn entry through repair/archive/durable coverage checks, before fast route construction |
| `route_and_construct` | Fast route entry through completion of the frame's output vector, including validation and all output construction |
| `construct` | Construction of this individual fast update; nested inside `route_and_construct` |
| `prepare_delivery` | This output's execution ticket, cold identity and deduplication bookkeeping before dispatch |
| `publish_started_ns` | Dispatch's enqueue attempt begins, after probe filtering and any enabled debug diagnostic |
| `thread_id` | Linux TID or Darwin thread identity of the current routing turn |

The original `private_owner_dequeued_ns` remains the first ingress boundary
across a cold archive/repair round trip. These CPU/wall spans measure the current
turn instead of attributing the intervening cold wait to current-thread CPU.
Frame-wide route durations are shared by maker fanout outputs and must not be
summed across those outputs. `construct` is nested and must not be added to its
parent. Work between frame routing and individual preparation is not a uniquely
measured substage; the whole-batch benchmark measures that work as well.

After ownership transfers to the lane, the sender cannot modify the immutable
message with its enqueue completion. The predeclared background telemetry
stages `polymarket.user.dispatch_enqueue.wall` and `.cpu` therefore measure
enqueue entry through return separately. They include existing routing/recovery
branches and bounded queue waiting. `publish_started_ns` is not the successful
enqueue time and does not by itself isolate pure queue residence.

The legacy startup/recovery enrollment buffer persists `OrderUpdate` rather
than `LifecycleTiming`. Its messages retain default/unknown timings; this
change does not invent original receipts or CPU spans for that path. Normal
owner delivery and overflow/retry retain their receipt and current-turn spans.

No runtime thread, queue capacity, routing authority or economic transition is
added. Existing replay suppression commits only after successful delivery.
The overflow/retry test fills the root lane, confirms no suppression commit,
drains it, delivers the original event, then confirms duplicate suppression.

The new field is appended to `LifecycleTiming` because historical MessagePack
records use positional arrays. Five-field and eight-field recordings keep their
old boundaries and decode new CPU spans as unknown. Named JSON recordings also
default missing stages. Linux and Darwin use `CLOCK_THREAD_CPUTIME_ID`; other
targets return an unavailable CPU clock.

`offline-private-owner-mimalloc` changes only the SDK libtest allocator through
the existing counting allocator. It uses mimalloc 0.1.52 and sys 0.1.49 extended;
consumer application allocator selection is unchanged. The ignored benchmark
uses a known order/trade fixture, warm execution cache and a capacity-one root
lane. It times batch entry through dispatch, excludes input construction,
receiver consumption, cold account apply and JSON export, and retains paired
CPU/wall samples. Local Darwin results do not represent maker02 ARM/Linux,
FIFO affinity, network or exchange end-to-end latency.
