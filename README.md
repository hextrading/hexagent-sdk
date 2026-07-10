# hexagent-sdk

A reusable **trading-system SDK** in Rust. It provides the runtime that connects
to exchanges, manages orders/positions, runs a first-principles backtest
simulator, and drives strategies through a clean `Strategy` interface — so a
strategy author writes only their alpha, not the plumbing. A Python SDK is
planned on top of the same boundary.

Strategies live **outside** this repo and depend on these crates via a pinned
git rev. The reference consumers are in the `hexbot` repo: `polymaker`
(Polymarket binary-option MM, flagship) plus the `hypermaker` / `astermaker` /
`litmaker` perp makers and the `strategies` bundle.

## Crates

| Crate | Responsibility |
|---|---|
| `hexagent-types` | Market events, strategy signals, instruments, orders, sim clock |
| `hexagent-config` | Layered TOML config + secrets loading |
| `hexagent-runtime` | OS tuning / core pinning, async runtime helper, latency instrumentation + record/replay, HTTP/1.1 role pool (`http1_pool`) |
| `hexagent-account` | Order manager, position manager, local orderbook |
| `hexagent-strategy` | The `Strategy` trait + `StrategyFactory` / `StrategyRegistry` / `StrategyCapabilities` (the contract) |
| `hexagent-exchange` | Exchange adapters + `ExchangeMarket`/`ExchangeTrade` traits, sim_v2 backtest simulator, parquet recorder |
| `hexagent-engine` | Engine runtime — live / paper / record / backtest, registry-driven, multi-instance/multi-account routing, Polymarket admission-control execution |

Internal dependency edges:

```
types ← (leaf)                     config ← (leaf)
runtime ← config                   account ← types, runtime
exchange ← types, config, account, runtime
strategy ← types, config, exchange     ← the factory exposes exchange handle types
engine ← all six
```

See [docs/architecture.md](docs/architecture.md) for the detailed map (engine
threading, admission control, adapter matrix, account semantics, sim_v2).

## Exchange coverage

- **Execution + market data**: polymarket (CLOB v2, signature types
  EOA/proxy/safe/1271), hexmarket, hyperliquid, lighter, aster, binance, plus a
  `paper` executor.
- **Market data only**: coinbase, bybit, okx, kraken, kucoin, gate, bitget,
  mexc, chainlink RTDS, pyth.
- **Backtest**: `sim_v2` discrete-event simulator (queue-based fills,
  maker/taker latency race, RTT record-replay; byte-identical run-to-run).

## Using the SDK

Implement `hexagent_strategy::strategy::Strategy` for your strategy, provide a
`hexagent_strategy::factory::StrategyFactory` (declare `capabilities()`, use
`inject_config()` to add the feeds you need), register it in a
`StrategyRegistry`, and hand that to `hexagent_engine::engine::Engine::new`.
The engine selects live / paper / record / backtest from `general.mode` in the
config. See the `polymaker` crate in the hexbot repo for a full example.

## Build

```sh
cargo build            # all SDK library crates
cargo test             # unit tests
```
