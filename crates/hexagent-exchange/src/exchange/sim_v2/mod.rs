//! Sim Exchange v2 — first-principles backtest simulator with independent
//! server and strategy-local lanes plus bidirectional RTT latency.
//!
//! Design: `docs/sim_v2_design.md`. P1 plan:
//! `~/.claude/plans/misty-squishing-galaxy.md`.
//!
//! # Two independent logical clocks
//! - **Strat lane** (`local_timestamp_ns`): engine-owned; drives strategy
//!   callbacks at the recorded receive time (faithful inbound — the recording
//!   already bakes in that day's real L2 market-data latency).
//! - **Server lane** (`exchange_timestamp_ns`): owned here; drives the matching
//!   core. Books carry a real server ts; trades are reconstructed by anchoring
//!   to the adjacent book (`feed.rs`).
//!
//! Both timestamps use the Unix epoch, which lets the coordinator order causal
//! arrivals, but the lanes never advance each other's logical clock. Separate
//! schedulers carry outbound requests to the server (`emit + L1`) and inbound
//! acks/fills to the strategy (`server_event + L2/private_push`).
//!
//! # RTT (P1)
//! `submit()` samples one RTT per signal, schedules `OrderReachesEngine` on the
//! server lane at `emit + L1`; processing it produces an ack scheduled on the
//! strategy lane at `reach + L2`. Private fills similarly cross via a sampled
//! push latency.
//!
//! # Deferred
//! P2: real book + cross-outcome synthetic book + taker. P3: resting queue
//! model. P4: timeout/orphan + RTT calibration refinement.

pub mod book;
pub mod clock;
pub mod event;
pub mod exchange;
pub mod feed;
pub mod latency;
pub mod simulator;
pub mod wallet;

pub use simulator::{SimV2Config, Simulator};
