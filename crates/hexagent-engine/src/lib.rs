//! hexagent-sdk — the engine runtime.
//!
//! Drives strategies through their lifecycle in live / paper / backtest modes,
//! wiring market-data feeds, the execution path, and the backtest simulator.
//!
//! The engine is strategy-agnostic: it constructs strategies through the
//! `hexagent_strategy::factory::StrategyRegistry` the application hands to
//! [`engine::Engine::new`], and never names a concrete strategy type.
//!
//! SDK re-exports keep engine.rs's `crate::…` module paths resolving.

pub use hexagent_types::types;
pub use hexagent_config::config;
pub use hexagent_account::account;
pub use hexagent_exchange::{exchange, recorder};
pub use hexagent_runtime::{async_rt, latency, latency_record, os_tune};

pub mod strategy {
    pub use hexagent_strategy::strategy::*;
}

pub mod engine;
