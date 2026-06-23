//! hexagent-sdk — exchange access, execution, and backtest simulation.
//!
//! The `exchange` module defines the `ExchangeMarket` (market-data feed) and
//! `ExchangeTrade` (order execution) traits and the per-venue adapters
//! (Binance, Coinbase, Polymarket, …) plus the first-principles backtest
//! simulator (`sim_v2`) and paper executor. `recorder` is market-data
//! record/replay; `index_price` is the cross-venue mid-price aggregator.
//!
//! Re-exports of the lower SDK crates keep the moved code's `crate::types::…`,
//! `crate::config::…`, `crate::account::…`, `crate::async_rt::…` etc. paths
//! resolving unchanged.

pub use hexagent_types::types;
pub use hexagent_config::config;
pub use hexagent_account::account;
pub use hexagent_runtime::{async_rt, latency, latency_record, os_tune};

pub mod exchange;
pub mod index_price;
pub mod recorder;
