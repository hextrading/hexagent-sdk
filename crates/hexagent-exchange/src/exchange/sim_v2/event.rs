//! Discrete-event types for the sim_v2 unified wall-clock scheduler.
//!
//! All events live on a single wall-clock time axis (see module docs in
//! `mod.rs`). Market events carry their server timestamp (`exchange_ts`,
//! trades reconstructed in `feed.rs`); my-order lifecycle events carry the
//! wall-clock time at which the order reaches the engine (`emit + L1`) or the
//! ack/fill reaches the strategy (`reach + L2`).

use crate::types::{
    Exchange, Instrument, OrderBookSnapshot, OrderRequest, OrderUpdate, TickSizeChange, TradeTick,
};

/// An action that, after the outbound L1 latency, reaches the matching core.
/// Batch signals expand into several of these at `submit` time, sharing one
/// sampled RTT (a batch is a single API call).
#[derive(Debug, Clone)]
pub enum ReachAction {
    Place(OrderRequest),
    Cancel {
        exchange: Exchange,
        client_order_id: String,
    },
    CancelAll {
        exchange: Exchange,
        symbol: String,
    },
}

/// The heap payload. Ordering is by `(when, seq)` only — see `clock.rs`.
#[derive(Debug, Clone)]
pub enum SimEvent {
    /// Server-axis book snapshot (real `exchange_timestamp_ns`).
    ServerBook(OrderBookSnapshot),
    /// Server-axis trade with reconstructed server ts (feed.rs overwrites
    /// `TradeTick.exchange_timestamp_ns` before this is constructed).
    ServerTrade(TradeTick),
    /// Instrument metadata (carries recorded local ts as `when`).
    ServerInstrument(Instrument),
    /// Tick-size change (carries recorded local ts as `when`).
    ServerTickSize(TickSizeChange),

    /// My order/cancel arrives at the matching core (`when = emit + L1`).
    /// `l2_ns` is the inbound latency to stash for delivering the ack.
    /// `suppress_ack` is set when the round-trip exceeded `client_timeout`:
    /// the order still reaches the engine (rests/fills) but its Accepted/
    /// Rejected/Cancelled ack is suppressed — the strategy already received a
    /// NewOrder/CancelOrderTimeout and will reconcile. Fills are always
    /// delivered (the strategy must learn of them).
    OrderReachesEngine { action: ReachAction, l2_ns: u64, suppress_ack: bool },
    /// A marketable (taker) order's actual book-match, deferred to the MIDPOINT
    /// of the matching window (`when = reach + overhead/2`) so the book can move
    /// in-flight — a taker that no longer crosses by then naturally misses and
    /// rests. The residual `overhead/2 + L2` carries it to the ack.
    TakerMatch {
        order: OrderRequest,
        l2_ns: u64,
        overhead_ns: u64,
        suppress_ack: bool,
    },
    /// An ack (Accepted / Cancelled / Rejected) due for strategy delivery
    /// (`when = reach + L2`).
    AckToStrategy(OrderUpdate),
    /// A fill due for strategy delivery (`when = match + L2`). Not constructed
    /// in P1 (matching is stubbed); the variant exists so P2 drops in cleanly.
    #[allow(dead_code)]
    FillToStrategy(OrderUpdate),
}
