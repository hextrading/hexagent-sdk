# Replay clocks, transport stages and resting liquidity

Replay consumes the caller's already calibrated public receive timestamps.
Hexbot's default is the four-platform record02→maker02 pure mean calibration,
before strict merge/window/warmup, retaining live 40ms / 2300ms pricing lags.
The SDK does not apply a second receive shift or alter private-event clocks.

This change normalizes recorded book level orientation without changing price,
quantity, duplicate events or source/receive clocks. Maker audit rows drain as
orders retire, with a lossless final completeness check, instead of retaining an
entire long run for formatted logging.

Cancel evidence distinguishes outbound transport, processing before effect,
work after effect, and return transport. Post-effect work delays the response
without extending fill eligibility. Client transport intervals stay explicitly
modeled evidence; they do not certify exchange timestamps. Repeated attempts
join by full owner/order/attempt identity before replay. Timeout evidence remains
censored and cannot supply invented response bounds.

Admission audits distinguish missing executable depth from observed zero depth
and only call an actual positive fill a taker match. The optional joint model
can sample immediate/full/partial quantities. Eventual maker-fill probability
is diagnostic and cannot disable an accepted GTC remainder: that remainder
stays resting and eligible for later native trade candidates until cancellation.
Research selection remains opt-in; none of these tests validates PnL convergence.

`sim_v2_decision_replay_path` is an optional offline JSONL schedule containing
`iid`, `callback_ns`, `decision_ns`, and `event_start_ns`. The named owner receives
the original callback at the explicit decision time, after equal-time ingress.
Normal public quote cadence is disabled only for scheduled owners. Private
lifecycle requotes remain separate. Unknown owners, duplicate/regressing times,
future/out-of-window callbacks, empty schedules and the two-million-row capacity
limit fail closed. This mode cannot combine with virtual owner scheduling and
is a same-decision diagnostic, not autonomous strategy performance evidence.

## Ownership and measurements

New runtime fields belong to the existing offline simulation coordinator/core.
Schedules, audit buffers and transport joins do not add live workers, shared
account authority or cross-thread request/reply calls. Strategy callbacks retain
their existing per-owner lifecycle lane. Calibration and JSON/file operations
are outside live quote processing. Existing legacy simulator maps/allocations
are not being promoted into live strategy ownership; further replay allocation
work should remain coordinator-local rather than introduce shared runtime maps.

Current-main checks: sim_v2 278 passed / 19 optional ignored; engine 178 passed /
10 ignored plus a subsequent decision capacity test; recorder reader 25 passed /
1 ignored; replay-command example 5 passed / 1 optional ignored. Coverage includes
instance isolation, ordering, idempotence, timeout/cancel races, bounded overflow,
book replay, partial fills and future maker eligibility.

Local debug microbenchmark, 100,000 samples/arm, boundary `Selection::select`
including bounded diagnostic enqueue, excluding matching and filesystem writes:

| Joint model | Median ns | P99 ns | P999 ns | Maximum ns | Queue high water / overflow |
| --- | ---: | ---: | ---: | ---: | --- |
| off | 1517 | 1606 | 15848 | 124126 | 128 / 0 |
| on | 3414 | 17654 | 23463 | 49091 | 128 / 0 |

This optional offline model incurs extra work; it is not a live latency
optimization. No live end-to-end latency improvement or PnL fit is claimed.
