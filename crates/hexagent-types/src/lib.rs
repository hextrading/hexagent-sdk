//! hexagent-sdk — shared trading-domain types.
//!
//! Foundation crate of the SDK: market events, strategy signals, instruments,
//! orders, and the simulated clock. No internal (intra-workspace) dependencies.
//!
//! The whole surface lives under the `types` module so that consumer crates
//! can re-export it as `pub use hexagent_types::types;` and keep their existing
//! `crate::types::…` paths resolving unchanged.

pub mod types;
