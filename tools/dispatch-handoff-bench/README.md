# Dispatcher scheduling probes

These standalone probes contain no trading executor or network client. The
handoff probe imports the SDK's actual `try_queue`, `poll_channel`, `Wake`, and
`Signal` sources. It carries the current fixed-size causal trace and validates
exactly-once, ordered event IDs. The two other binaries exercise the frozen
75592d1 admission implementation versus the current source, and the old/current
wire representations extracted from `trade.rs`.

Build with `cargo build --release --manifest-path tools/dispatch-handoff-bench/Cargo.toml`.
Run queue/receipt regressions with `cargo test --manifest-path tools/dispatch-handoff-bench/Cargo.toml --bin dispatch-handoff-bench`.

On an explicitly reserved Linux test CPU pair:

```sh
sudo tools/dispatch-handoff-bench/target/release/dispatch-handoff-bench 0 1 30000 fifo 1 165 55
nice -n 19 tools/dispatch-handoff-bench/target/release/admission
nice -n 19 tools/dispatch-handoff-bench/target/release/wire
```

Handoff arguments are producer CPU, alternative dispatcher CPU, number of
orders, scheduling (`fifo` enables real FIFO), owner/I/O CPU, router work in
microseconds, and preparation work in microseconds. Each of four modes receives
the same three-order bursts every millisecond. Priorities are producer 60,
dispatcher/owner 50, I/O stub 70. `isolated` means separated from the producer;
the owner/I/O stub still shares its CPU with that dispatcher in this two-CPU
fixture. The I/O stub uses event wakeups in every mode, holding that lane
constant. Queue capacity is 1024 per lane. Overflow aborts immediately; queue
high-water marks are sampled after publication, not exact instantaneous peaks.

Every measured interval begins at a common synthetic receive `Instant` for
its three-order burst. JSON output reports N, median, P99/P999 (nearest rank),
maximum, queue depths/overflow, affinity, payload sizes, and workload. Sample
storage is preallocated and sorting/printing happens after the measured loop.
The first-write endpoint is an I/O **stub**. It contains neither ECDSA nor
HTTP/TLS/socket I/O, so these numbers are mechanism comparisons, not live
exchange latency or a prediction of production P99. Production tracing
separately measures actual HTTP first-write and completion boundaries.

The admission probe reports four Fast eligibility checks, including one fresh
lane observation when `with_observation=true`. It uses the actual algorithms
with small immutable transport-type definitions, without an exchange runtime.
The wire probe uses counted mimalloc, one reusable 2048-byte output buffer and
a fixed test signature, asserts byte-identical JSON, and measures construction,
serialization and drop. It does not measure cryptographic signing.
