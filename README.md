# hexagent-sdk

A reusable **trading-system SDK** in Rust. It provides the runtime that connects
to exchanges, manages orders/positions, runs a first-principles backtest
simulator, and drives strategies through a clean `Strategy` interface — so a
strategy author writes only their alpha, not the plumbing. A Python SDK is
planned on top of the same boundary.

Strategies live **outside** this repo and depend on these crates. The reference
example is `polymaker` (a Polymarket market maker) in the `hexbot` repo.

## Crates

| Crate | Responsibility |
|---|---|
| `hexagent-types` | Market events, strategy signals, instruments, orders, sim clock |
| `hexagent-config` | Layered TOML config + secrets loading |
| `hexagent-strategy` | The `Strategy` trait + `StrategyFactory` / `StrategyRegistry` (the contract) |
| `hexagent-runtime` | OS tuning, async runtime helper, latency instrumentation + record/replay |
| `hexagent-account` | Order manager, position manager, local orderbook |
| `hexagent-exchange` | Exchange adapters + `ExchangeMarket`/`ExchangeTrade` traits, sim_v2 backtest, recorder, index price |
| `hexagent-engine` | Engine runtime — live / paper / backtest, registry-driven |

Dependency direction (low → high):
`types → config → {strategy, runtime} → account → exchange → engine`.

## Using the SDK

Implement `hexagent_strategy::strategy::Strategy` for your strategy, provide a
`hexagent_strategy::factory::StrategyFactory`, register it in a
`StrategyRegistry`, and hand that to `hexagent_engine::engine::Engine::new`.
See the `polymaker` crate in the hexbot repo for a full example.

## Build

```sh
cargo build            # all SDK library crates
cargo test             # unit tests
```
