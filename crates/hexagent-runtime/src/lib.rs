//! hexagent-sdk — low-level runtime utilities.
//!
//! OS latency tuning (`os_tune`), the async runtime helper (`async_rt`),
//! latency instrumentation (`latency`) and per-request latency record/replay
//! (`latency_record`). Foundational layer used by the account, exchange and
//! engine crates.
//!
//! `pub use hexagent_config::config;` keeps `os_tune`'s `crate::config::…`
//! paths resolving; the modules reference each other via `crate::os_tune` etc.

pub use hexagent_config::config;

pub mod async_rt;
pub mod background_jobs;
pub mod http1_pool;
pub mod latency;
pub mod latency_record;
pub mod os_tune;
