//! Fixed-size, immutable causal receipt and single-owner execution timestamps.
//! No routing/risk authority. Zero is unknown, including legacy/replayed data.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketReceipt {
    pub clock_domain_ns: u64,
    pub feed_session_id: u64,
    pub message_sequence: u64,
    pub ws_received_unix_ns: u64,
    pub ws_received_mono_ns: u64,
    pub parsed_mono_ns: u64,
}

/// One feed task owns this sequence; construct a new session on reconnect.
pub struct ReceiptSequencer {
    session: u64,
    sequence: u64,
}
impl Default for ReceiptSequencer {
    fn default() -> Self {
        Self::new()
    }
}
impl ReceiptSequencer {
    pub fn new() -> Self {
        let _ = super::monotonic_clock_domain_ns();
        // Reconnect identity is allocated only on feed setup, never per event.
        static NEXT_SESSION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self {
            session: NEXT_SESSION.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            sequence: 0,
        }
    }
    pub fn received(&mut self) -> MarketReceipt {
        if self.sequence == u64::MAX {
            return MarketReceipt::default();
        }
        self.sequence += 1;
        let mono = super::monotonic_now_ns();
        MarketReceipt {
            clock_domain_ns: super::monotonic_clock_domain_ns(),
            feed_session_id: self.session,
            message_sequence: self.sequence,
            ws_received_unix_ns: super::now_ns(),
            ws_received_mono_ns: mono,
            parsed_mono_ns: 0,
        }
    }
}
impl MarketReceipt {
    pub fn parsed(mut self) -> Self {
        self.parsed_mono_ns = super::monotonic_now_ns();
        self
    }
    pub fn is_current(self) -> bool {
        self.clock_domain_ns == super::monotonic_clock_domain_ns()
            && self.ws_received_mono_ns != 0
            && self.feed_session_id != 0
            && self.message_sequence != 0
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotPathTrace {
    pub clock_domain_ns: u64,
    pub receipt: MarketReceipt,
    pub strategy_dequeued_mono_ns: u64,
    pub signal_mono_ns: u64,
    pub executor_received_mono_ns: u64,
    pub owner_published_mono_ns: u64,
    pub owner_dequeued_mono_ns: u64,
    pub prep_mono_ns: u64,
    pub signed_mono_ns: u64,
    pub account_recorded_mono_ns: u64,
    pub http_submitted_mono_ns: u64,
}
impl HotPathTrace {
    /// Unknown/replay/clock-domain mismatch remains null, never fake zero.
    pub fn receive_to_dispatch_ns(self) -> Option<u64> {
        if !self.receipt.is_current() || self.clock_domain_ns != self.receipt.clock_domain_ns {
            return None;
        }
        self.http_submitted_mono_ns
            .checked_sub(self.receipt.ws_received_mono_ns)
            .filter(|_| self.http_submitted_mono_ns != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sessions_sequences_and_replay_are_explicit() {
        let mut a = ReceiptSequencer::new();
        let one = a.received().parsed();
        let two = a.received().parsed();
        assert_eq!(one.feed_session_id, two.feed_session_id);
        assert_eq!(two.message_sequence, one.message_sequence + 1);
        assert!(two.parsed_mono_ns >= two.ws_received_mono_ns);
        let mut b = ReceiptSequencer::new();
        assert_ne!(one.feed_session_id, b.received().feed_session_id);
        let trace = HotPathTrace {
            receipt: one,
            clock_domain_ns: one.clock_domain_ns,
            http_submitted_mono_ns: super::super::monotonic_now_ns(),
            ..Default::default()
        };
        assert!(trace.receive_to_dispatch_ns().is_some());
        let old = HotPathTrace {
            receipt: MarketReceipt {
                clock_domain_ns: 1,
                ..one
            },
            ..trace
        };
        assert_eq!(old.receive_to_dispatch_ns(), None);
        assert_eq!(HotPathTrace::default().receive_to_dispatch_ns(), None);
    }
    #[test]
    fn old_compact_books_and_quote_delivery_decode_as_unknown() {
        use super::super::{Exchange, OrderBookSnapshot, PriceLevel, QuoteDelivery, QuoteOrigin};
        let old = (
            Exchange::Binance,
            "BTCUSDT",
            Vec::<PriceLevel>::new(),
            Vec::<PriceLevel>::new(),
            123_u64,
            456_u64,
        );
        let encoded = rmp_serde::to_vec(&old).unwrap();
        let decoded: OrderBookSnapshot = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.symbol, "BTCUSDT");
        assert_eq!(decoded.local_timestamp_ns, 456);
        assert_eq!(decoded.receipt, MarketReceipt::default());
        let old = rmp_serde::to_vec(&(QuoteOrigin::Wire, 789_u64)).unwrap();
        let decoded: QuoteDelivery = rmp_serde::from_slice(&old).unwrap();
        assert_eq!(decoded.published_timestamp_ns, 789);
        assert_eq!(decoded.receipt, MarketReceipt::default());
    }

    #[test]
    fn appended_trace_preserves_old_compact_order_fields() {
        let mut order = super::super::OrderRequest::new_limit(
            super::super::Exchange::Polymarket,
            "42".into(),
            super::super::Side::Buy,
            0.5,
            20.0,
        );
        order.instance_id = "owner-a".into();
        order.outcome_label = "Up".into();
        let encoded = rmp_serde::to_vec(&order).unwrap();
        let mut fields: Vec<serde_json::Value> = rmp_serde::from_slice(&encoded).unwrap();
        fields.pop().unwrap();
        let old = rmp_serde::to_vec(&fields).unwrap();
        let restored: super::super::OrderRequest = rmp_serde::from_slice(&old).unwrap();
        assert_eq!(restored.instance_id, "owner-a");
        assert_eq!(restored.outcome_label, "Up");
        assert_eq!(restored.price, order.price);
        assert_eq!(restored.client_order_id, order.client_order_id);
        assert_eq!(restored.hot_path, HotPathTrace::default());
    }
}
