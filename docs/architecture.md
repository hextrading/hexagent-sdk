# hexagent-sdk architecture

**Updated**: 2026-07-10 (main `218cb6f`). Workspace of 7 library crates
(`members = ["crates/*"]`, release profile `lto = "fat"`, `codegen-units = 1`).
Strategies live outside this repo (hexbot) and consume the SDK via a pinned git
rev.

## 1. Crate map

```
hexagent-types      ← (leaf)                     MarketEvent / Signal / Instrument / orders + sim clock
hexagent-config     ← (leaf)                     layered TOML config + secrets
hexagent-runtime    ← config                     os_tune (core pinning / SCHED_FIFO / mlockall),
                                                 async_rt, latency record/replay, http1_pool
hexagent-account    ← types, runtime             OrderManager / PositionManager / OrderbookManager
hexagent-exchange   ← types, config, account,    exchange adapters + ExchangeMarket/ExchangeTrade
                      runtime                    traits + sim_v2 backtest + parquet recorder
hexagent-strategy   ← types, config, exchange    Strategy trait + Factory/Registry/Capabilities
hexagent-engine     ← all six                    live / paper / record / backtest runtime
```

Notes:

- `hexagent-strategy` sits **above** `hexagent-exchange`: the factory's build
  deps expose Polymarket `SharedState` / `rtt_probe::ActiveTokenHandle` types
  (`factory.rs`).
- `hexagent-exchange` re-exports `async_rt` / `http1_pool` / `latency` /
  `os_tune` and types/config/account so code moved during the SDK split keeps
  its original `crate::…` paths.
- `index_price` (the myindex aggregator) is **not** in the SDK — it moved back
  to the strategy layer (each strategy crate owns a copy); only the
  `parse_index_exchanges` factory helper remains here.

## 2. Strategy contract (`hexagent-strategy`)

**`Strategy` trait** (`strategy.rs`, `Send`):

- Identity/routing: `name()`, `instance_id()` (tags tracing spans),
  `subscribed_symbols()` (drives the per-instance market router).
- Market-data callbacks → `Vec<Signal>`: `on_orderbook`, `on_trade_tick`,
  `on_quote_tick`, `on_bar`, `on_spot_price`, `on_instrument`,
  `on_tick_size_change`.
- Quote cadence: `on_quote(ts)`, `quote_interval_ms`,
  `quote_trigger_binance_ob_only`, `quote_interval_tolerance_frac`,
  `quote_tick_by_tick`, `cadence_rtt_throttle`.
- Lifecycle/execution: `on_connected` / `on_disconnected`,
  `on_order_update` (may synchronously return `ReconcilePolymarket`),
  `on_init` / `on_exit` / `on_shutdown`.
- Backtest / warm-up hooks: `load_hist_data` / `on_hist_bar` /
  `on_hist_data_loaded`, prediction warm-up, apv2 activity-baseline warm-up
  (`on_apv2_warmup_orderbook/_trade`, `apv2_warmup_resume_ns`,
  `apv2_warmup_finalize_cache`), `set_per_event_prev_p_override`.
- `dispatch_in_span` wraps each dispatch in a `strat{iid=…}` span.

**Factory/registry** (`factory.rs`):

- `StrategyBuildDeps`: strategy cfg + full config + `bt_start_ns` +
  `strategy_index` + optional live Polymarket handles (`rtt_probe`,
  `stale_threshold`, `poly_state`).
- `StrategyFactory`: `name()` / `build(deps)` / `capabilities()` /
  **`inject_config(cfg, &mut full)`** — mutates the config to add required
  feeds before exchange threads spawn (replaces the old hard-coded
  `inject_*_symbols`).
- `StrategyCapabilities` — 5 flags: `needs_rtt_probe`, `needs_hist_bars`,
  `needs_sim_wallet`, `needs_poly_user_feed`, `needs_hex_workers`. All former
  `name == "polymaker"` engine gates are capability queries now.
- `StrategyRegistry`: `register` / `build` / `capabilities(name)` /
  `inject_all_config`. The consumer bin registers factories and hands the
  registry to `Engine::new` — the engine names no concrete strategy.

## 3. Engine (`hexagent-engine`)

`Engine::run()` dispatches on `general.mode`: `run_live` / `run_paper` /
`run_record` / `run_backtest`.

### 3.1 Multi-instance / multi-account (live & paper)

- **One core-pinned worker thread per strategy instance**
  (`spawn_per_instance_strategy_threads`, cores from
  `[os_tune].strategy_cores[instance_id]`) plus a `strategy-router` thread.
  Single-instance and backtest keep the single-threaded bit-exact path.
- **Market routing**: the router fans each `MarketEvent` only to instances
  whose `subscribed_symbols` match (`sym_to_instances`).
- **coid → instance routing**: workers register placed coids in a shared
  `coid_owner` map; on an `OrderUpdate` the router looks up the owner, falling
  back to parsing the **`{iid}-{n}` coid prefix** (for late reconcile-driven
  synthetic updates). Terminal updates evict the entry. The prefix is enabled
  per-OrderManager via `set_coid_prefix`.
- **Account decoupling**: a Polymarket account = one `SharedState`
  (`[poly.<account_id>]` secrets block). Instances may share a wallet Arc;
  `dedup_states_by_account` collapses to one `(instance_id, Arc)` per unique
  Arc so per-account tasks (user feed, heartbeat) never double-open streams
  (owner = lexicographically-smallest instance_id).

### 3.2 Polymarket execution: admission control + fire-and-track

- **`http1_pool`** (`hexagent-runtime/src/http1_pool.rs`): per-(instance,
  role) connection pools, `Role ∈ {Fast, Cancel, Reconcile, Query}` (default
  Fast=Cancel=3, Reconcile=Query=1 per instance). A `Permit` reserves a warm
  connection and releases on `Drop` — **permit count = warm connections, no
  concurrent cold connections**. Instance A exhausting Fast never blocks B; a
  full Cancel pool never blocks a Fast place.
- **Fire-and-track** (`fire_or_execute`): acquire a permit → fire the request
  **without blocking** → hand the reply closure to a drainer
  (`PolyCompletionFn`). **No permit ⇒ SKIP** (emit `ExecutorRejected`; the
  strategy retries next tick) — no queueing, no backlog.
- Replace path: `Role::Cancel` gates `Role::Fast` (cancel first, then place).
  Reconcile runs on the disjoint `Role::Reconcile` pool so it can't steal
  hot-path capacity. A `poly-admission-stats` background thread logs
  per-(instance, role) acquire/skip/busy deltas.

### 3.3 Thread / core-pinning model

`os_tune` classes: `pin_strategy` (workers + router), `pin_execution`
(exec workers, feed threads, poly dispatch/worker threads), `pin_background`
(recorder, latency-flush, admission-stats). Deployment targets isolated cores
(isolcpus / nohz_full), CAP_SYS_NICE + CAP_IPC_LOCK.

## 4. Exchange adapters (`hexagent-exchange`)

Two traits: `ExchangeMarket` (market data, blocking `next_event`) and
`ExchangeTrade` (submit / cancel / cancel_all / batch / replace). Shared
`ReconnectBackoff` with jitter.

| Adapter | Market data | Execution | User feed |
|---|---|---|---|
| polymarket | ✓ | ✓ | ✓ (per-account) |
| hexmarket / hyperliquid / lighter / aster | ✓ | ✓ | ✓ |
| binance | ✓ (+kline) | ✓ | — |
| coinbase / bybit / okx / kraken / kucoin / gate / bitget / mexc (protobuf) | ✓ | — | — |
| chainlink RTDS / pyth | ✓ (oracle) | — | — |
| paper | — | ✓ (simulated) | — |
| sim_v2 | backtest simulator | | |

### 4.1 Polymarket adapter

- **CLOB v2**: `ClobVersion::{V1, V2}`, default V2 (`clob_base_url` must match
  the v2 host). `sign_and_build_body` dispatches on version.
- **Signature types** (`signer.rs`): `Eoa = 0` (first-class since 2026-07),
  `PolyProxy = 1`, `PolyGnosisSafe = 2`, `Poly1271 = 3` (deposit wallet,
  ERC-7739 wrap, maker == signer == funder).
- **Salt**: per-account monotonic — counters keyed by lowercased maker
  address, high 32 bits = process start second; never reused, always
  increasing.
- **User feed** (`user_feed.rs`): per-account authenticated WS task with
  **gap-replay** (`/trades?after=` catch-up on a periodic cadence and on
  reconnect rewind), PING keepalive + watchdog. Balance/inventory sync is
  keyed by `account_id` (`LivePositionManager`, `TakerMatchedInventory`,
  `UserFeedHealth`).
- **On-chain maintenance** (`onchain_tx.rs`, `merge.rs`): redeem / split /
  merge serialized behind a `MaintenanceStatus` state machine; startup top-up
  runs first. Auto-redeem is default-OFF in code (enabled per deployment).
- **Polygon RPC pool**: `[polygon].rpc_list` — round-robin starting offset,
  walks the whole pool for full failover.
- Watchdogs: CLOB WS 90 s stale threshold → reconnect; chainlink RTDS 30 s
  keepalive ping + stall watchdog + reconnect backoff.

## 5. Account (`hexagent-account`)

- **OrderManager**: tracks `LocalOrder` by coid; coid seed = wall-clock ms in
  live vs fixed seed in backtest (byte-identical runs); optional
  `{instance_id}-` coid prefix; `on_signal_dropped` handles admission-SKIPped
  placements.
- **PositionManager**: positions / inventory / balance, `upsert_trade`,
  pending-order registry, available cash/inventory including locks.
- **OrderbookManager**: per-symbol local books, best bid/ask, mid, spread.
- **Mappings survive rejects**: a rejected order stays `Rejected` in the OM
  and Polymarket `coid↔oid↔token` maps are kept — a racy reject/cancel can
  still be followed by a real fill. Maps are reclaimed per event at settlement
  via a grace-delayed `pending_reclaim` queue.

## 6. Backtest simulator (`sim_v2`)

First-principles discrete-event simulator on one wall-clock axis with two
lanes: the **strat lane** (`local_timestamp_ns`, engine-owned, replays recorded
receive times so real market-data latency is preserved) and the **server lane**
(`exchange_timestamp_ns`, matching-core-owned) — their offset is network
latency. A single `Scheduler` holds server market events, my-order arrivals
(emit+L1) and ack/fill deliveries (reach+L2); the engine merges against its
feed via `Simulator::peek_when`.

- Fills: queue-based resting book + **maker/taker race**
  (`maker_race_rate` / `taker_race_rate`) modelling latency-adverse pick-off;
  taker fills add a sampled matching-engine overhead on top of the place RTT.
- Latency: **record-replay** of per-request place/cancel RTT CSVs (or parquet
  archives); exact samples where available, otherwise a **date-aware
  fallback** (`rtt_sim_fallback`): exact time-of-day → nearest day within
  tolerance → nearest day's tod-bucket distribution (seed-deterministic).
- Coids are FNV-hashed into per-order Bernoullis — backtests are
  byte-identical run-to-run.

(Strategy-side sim_v2 design/calibration notes live in the hexbot repo:
`docs/sim_v2_design.md`, `docs/sim_v2_calibration.md`,
`docs/sim_v2_taker_physical.md`.)

## 7. Consumer pinning

Consumers pin the SDK by git rev, with the single source of truth in their
workspace `[workspace.dependencies]`. For local SDK development, add a
`[patch]` section with path overrides; to adopt SDK changes, bump the rev in
one place. When reading SDK source alongside a consumer, first check the local
checkout matches the consumer's pinned rev.
