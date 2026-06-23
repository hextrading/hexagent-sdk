//! hexagent-sdk — account bookkeeping.
//!
//! `OrderManager`, `PositionManager` and the local `OrderbookManager` —
//! strategy-facing state that tracks live orders, inventory and book.
//!
//! Re-exports keep `crate::types::…` and `crate::latency::…` paths resolving
//! inside the moved module.

pub use hexagent_types::types;
pub use hexagent_runtime::latency;

pub mod account;
