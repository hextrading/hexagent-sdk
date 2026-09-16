//! Discrete-event messages crossing sim_v2's server and strategy schedulers.
//!
//! Both lanes use epoch timestamps for causal coordination, but retain
//! independent monotonic clocks. Market events and request arrivals belong to
//! the server lane; acknowledgements and private fills belong to the strategy
//! lane after their modeled inbound latency.

use super::evidence::{ArrivalEvidence, BookContinuityRecord};
use super::timing::CancelTiming;
use crate::types::{
    Exchange, Instrument, OrderBookSnapshot, OrderRequest, OrderUpdate, Side, TickSizeChange,
    TradeTick,
};

/// Immutable replay clock evidence. `raw_source_ns` is the recorded payload
/// field, including its known limitation for public trades (receive time, not
/// a venue match timestamp). Only trades have a reconstructed source estimate.
/// `effective_apply_ns` is the monotonic DES clock; it must never replace book
/// source time in the strict profile. Stream/sequence identify replay order,
/// not a venue sequence or connection session (neither is in these records).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct ServerEventTime {
    pub raw_source_ns: u64,
    pub recorded_receive_ns: u64,
    pub reconstructed_source_ns: Option<u64>,
    pub effective_apply_ns: u64,
    pub stream_index: u32,
    pub stream_sequence: u64,
}

/// An action that, after the outbound L1 latency, reaches the matching core.
/// Batch signals expand into several of these at `submit` time, sharing one
/// sampled RTT (a batch is a single API call).
#[derive(Debug, Clone)]
pub enum ReachAction {
    Place(OrderRequest),
    /// A query is evaluated on arrival, with its response delayed independently.
    Reconcile {
        pending_places: Vec<(String, String, Side, f64, Option<String>)>,
        pending_cancels: Vec<(String, String)>,
        pending_trade_ids: Vec<String>,
    },
    Cancel {
        exchange: Exchange,
        client_order_id: String,
    },
    CancelAll {
        exchange: Exchange,
        symbol: String,
    },
    /// Opt-in, explicit account/market scope. Evaluated at cancel effective time.
    CancelAllOwned {
        exchange: Exchange,
        instance_id: String,
        symbols: Vec<String>,
    },
}

/// The heap payload. Ordering is by `(when, priority, seq)` — see `clock.rs`.
#[derive(Debug, Clone)]
pub enum SimEvent {
    /// Explicit offline protocol evidence, bound to an owner and actual token.
    BookContinuity(BookContinuityRecord),
    /// Strict profile preserves the payload's recorded source timestamp.
    /// Legacy profile keeps its historical rewritten payload; evidence is raw
    /// in both profiles so audit never needs to infer the original clock.
    ServerBook(OrderBookSnapshot, ServerEventTime),
    ServerTrade(TradeTick, ServerEventTime),
    /// Instrument metadata (carries recorded local ts as `when`).
    ServerInstrument(Instrument, ServerEventTime),
    /// Tick-size change (carries recorded local ts as `when`).
    ServerTickSize(TickSizeChange, ServerEventTime),

    /// My order/cancel arrives at the matching core (`when = emit + L1`).
    /// `l2_ns` is the inbound latency to stash for delivering the ack.
    /// `suppress_ack` is set when the round-trip exceeded `client_timeout`:
    /// the order still reaches the engine (rests/fills) but its Accepted/
    /// Rejected/Cancelled ack is suppressed — the strategy already received a
    /// NewOrder/CancelOrderTimeout and will reconcile. Fills are always
    /// delivered (the strategy must learn of them).
    OrderReachesEngine {
        action: ReachAction,
        arrival_evidence: ArrivalEvidence,
        l2_ns: u64,
        suppress_ack: bool,
        request_id: Option<u64>,
        cancel_timing: Option<CancelTiming>,
    },
    /// A cancel accepted by the API lane but not yet final in the matching
    /// engine. `ack_deliver_ns` remains the original reach+L2 response time;
    /// market events at the same timestamp win the scheduler tie.
    CancelFinalizes {
        exchange: Exchange,
        client_order_id: String,
        ack_deliver_ns: u64,
        suppress_ack: bool,
        request_id: Option<u64>,
        cancel_timing: Option<CancelTiming>,
    },
    CancelAllFinalizes {
        exchange: Exchange,
        instance_id: String,
        symbols: Vec<String>,
        timing: CancelTiming,
        suppress_ack: bool,
        request_id: u64,
    },
    /// One endpoint response projects its per-order results together. A timeout
    /// completes only the HTTP attempt; the exchange work remains scheduled.
    CancelAllHttpReply {
        request_id: u64,
        instance_id: String,
        updates: Vec<OrderUpdate>,
    },
    /// There is no per-order CancelAll timeout DTO in the public API. Keep its
    /// HTTP deadline explicit in audit without inventing a private order state.
    CancelAllDeadline {
        request_id: u64,
        instance_id: String,
    },
    /// A marketable (taker) order's actual book-match, deferred to the MIDPOINT
    /// of the matching window (`when = reach + overhead/2`) so the book can move
    /// in-flight — a taker that no longer crosses by then naturally misses and
    /// rests. The residual `overhead/2 + L2` carries it to the ack.
    TakerMatch {
        order: OrderRequest,
        arrival_ns: u64,
        arrival_evidence: ArrivalEvidence,
        l2_ns: u64,
        overhead_ns: u64,
        suppress_ack: bool,
        request_id: Option<u64>,
    },
    /// An ack (Accepted / Cancelled / Rejected) due for strategy delivery
    /// (`when = reach + L2`).
    AckToStrategy(OrderUpdate),
    /// Local admission rejection: no HTTP request was sent.
    AdmissionRejected(OrderUpdate),
    QueryTimeout(OrderUpdate),
    /// Opt-in request lifecycle, keyed by a simulator-local attempt identity.
    /// Reply and deadline may race; only one can complete the HTTP attempt.
    HttpReplyToStrategy {
        request_id: u64,
        update: OrderUpdate,
    },
    RequestDeadline {
        request_id: u64,
        timeout: OrderUpdate,
    },
    /// A private fill due for strategy delivery after its independent push delay.
    FillToStrategy(OrderUpdate),
    /// A private fill whose primary push was missed. The payload remains in
    /// the simulator-owned account recovery map and is delivered exactly once
    /// when this fallback fires or an earlier trade-id reconciliation finds it.
    PrivateFillRecovery {
        trade_id: String,
    },
}
