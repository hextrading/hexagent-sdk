//! hexagent-sdk — the strategy interface.
//!
//! Defines the [`strategy::Strategy`] trait every trading strategy implements,
//! the `dispatch_in_span` logging helper, and the [`factory`] registry by which
//! the engine constructs strategies WITHOUT knowing their concrete types. This
//! is the contract between the SDK engine and strategy crates (e.g. `polymaker`).
//!
//! `pub use hexagent_types::types;` keeps the trait's `crate::types::…` paths
//! resolving; consumer crates re-export `hexagent_strategy::strategy`.

pub use hexagent_types::types;
pub use hexagent_config::config;

pub mod strategy;
pub mod factory;
