//! hexagent-sdk — configuration.
//!
//! `Config` / `GeneralConfig` / `StrategyConfig` / `ExchangeConfig` /
//! `BacktestConfig` and the secrets-file loader. Strategy params are a generic
//! `toml::Value` map, so the SDK stays strategy-agnostic.
//!
//! Exposed under the `config` module for transparent `crate::config::…`
//! re-export by consumer crates.

pub mod config;
