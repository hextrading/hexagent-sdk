//! Sim Exchange v2 — first-principles backtest simulator (P1: feed + clock +
//! unified-wall-clock DES + bidirectional RTT latency; matching stubbed).
//!
//! Design: `docs/sim_v2_design.md`. P1 plan:
//! `~/.claude/plans/misty-squishing-galaxy.md`.
//!
//! # Two axes, one wall clock
//! - **Strat lane** (`local_timestamp_ns`): engine-owned; drives strategy
//!   callbacks at the recorded receive time (faithful inbound — the recording
//!   already bakes in that day's real L2 market-data latency).
//! - **Server lane** (`exchange_timestamp_ns`): owned here; drives the matching
//!   core. Books carry a real server ts; trades are reconstructed by anchoring
//!   to the adjacent book (`feed.rs`).
//!
//! Because `local_ts` and `exchange_ts` share the same wall clock (the offset
//! is the network latency), the `Scheduler` holds ALL internal events on one
//! wall-clock axis: server market events, my-order arrivals (`emit + L1`), and
//! ack/fill deliveries (`reach + L2`). The engine merges `Simulator::peek_when`
//! against its strat-lane feed.
//!
//! # RTT (P1)
//! `submit()` samples one RTT per signal, schedules `OrderReachesEngine` at
//! `emit + L1`; processing it produces an ack scheduled at `reach + L2`. So my
//! orders' acks reach the strategy after a full sampled RTT. Matching is a stub
//! (no fills); only ack/cancel paths carry latency (PnL = 0).
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
