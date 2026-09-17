# Optional venue-hold correction after the R3 experiment freeze

The earlier V6 replay attempts used the immutable `build-r3` binary and source
snapshot. Their profiles all have an empty `sim_v2_market_rules_path`. This
correction was implemented after that freeze and will be included in R4 with
the independent oracle-history recovery correction. The R3 source and binary
remain prior evidence; each experiment manifest identifies its own revision.
The optional-rule fix alone does not support a PnL improvement claim.

## Correctness issue and scope

Previously, a historical successful HTTP RTT shorter than a configured taker
hold aborted at dispatch for every non-post-only order. A passive GTC order can
legitimately have that short RTT because the taker hold does not apply. Whether
an order crosses is determined by the book at its modeled server arrival.

For this previously rejected case only, dispatch now keeps the ordinary RTT
partition and records that the potential hold has no reserved budget. At
arrival, a non-crossing order follows the ordinary admission path and receives
its reply at the original RTT. A crossing order fails explicitly before a
`TakerMatch`, private fill, or successful HTTP reply is scheduled. No future
book is inspected and no recorded RTT is stretched.

Feasible hold budgets retain their previous behavior. Empty rule journals and
all formal R3 profiles retain their previous numerical timing and matching
paths. This does not infer true venue arrival or resolve historical rule
availability: the optional journal still requires independent provenance, and
its rule lookup remains keyed to dispatch time. The existing feasible-budget
allocation remains a modeling assumption when the order arrives passive.

## Ownership and verification

`RuleHold::budget_reserved` is owned by the existing simulator server-lane
single writer in its already bounded hold map. No worker, queue, live critical
path, I/O, or cross-thread mutable state is added.

The focused `v6_market_hold` tests cover:

- A crossing order at dispatch that becomes passive before arrival, with a
  100 ms observed RTT and a 250 ms conditional hold: accepted at 100 ms, no
  fill, and unchanged arrival interval.
- A short-RTT crossing arrival: dispatch succeeds, arrival fails before
  matching or private/success delivery, and available quantity stays intact.
- Existing feasible hold matching, expiry revalidation, cancellation while
  held, and timeout lower-bound behavior.

Command, run from the SDK worktree:

```sh
cargo test --offline -p hexagent-exchange --lib v6_market_hold \
  --target-dir /Users/Admin1/projects/hexagent-sdk/target -- --nocapture
```

An independent recovery-gap issue requires a new R4 freeze and complete formal
matrix rerun. Those profiles still use empty rule journals. R3 test and replay
hashes continue to identify the earlier implementation; the focused tests and
incremental patch identify this later optional-rule correction.
