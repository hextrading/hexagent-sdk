use std::collections::HashMap;

use crate::types::OrderBookSnapshot;

/// One nudge per (symbol, side).
///
///   * `price`       — the asserted top-of-book bound (real bid ≥ price
///                     for Sell rejects; real ask ≤ price for Buy).
///   * `created_ns`  — local-clock timestamp at the moment we made the
///                     nudge. Compared against the **server-side**
///                     `exchange_timestamp_ns` of incoming WS snapshots
///                     to decide when the nudge is no longer needed:
///                     a snapshot may clear the nudge only when its
///                     `exchange_timestamp_ns` exceeds `created_ns` by
///                     at least `NUDGE_MIN_HOLD_NS`.
#[derive(Debug, Clone, Copy)]
struct Nudge {
    price: f64,
    created_ns: u64,
}

/// Minimum nudge hold: a snapshot clears a nudge only if its server-side
/// `exchange_timestamp_ns` is later than the nudge's `created_ns` by MORE
/// than this margin.
///
/// A bare `snapshot_ts > created_ns` check proved insufficient in live:
/// the snapshot's timestamp is assigned when the book publisher emits the
/// message, which can postdate the matching-engine state it was built
/// from. During fast moves Polymarket kept sending snapshots stamped
/// 60–100 ms after our nudge moment that still carried the pre-move top,
/// clearing the nudge and letting the strategy re-emit the same crossing
/// post-only price. Observed 2026-07-23 08:02:01 (coid btc02-
/// 1784778635245…248): SELL @ 0.41 rejected 4× in 300 ms, each repeat
/// enabled by a pseudo-fresh stale snapshot wiping the nudge. The 500 ms
/// margin absorbs that publisher pipeline lag plus modest NTP skew.
const NUDGE_MIN_HOLD_NS: u64 = 500_000_000;

/// Maintains the latest OrderBookSnapshot per symbol, plus
/// timestamp-anchored nudge overrides for cases where a server
/// rejection (e.g. post-only-crosses-book) reveals the real top is
/// ahead of our cached snapshot.
///
/// Stale WS snapshots — those generated server-side BEFORE the move
/// that produced the rejection — can update body levels but cannot
/// push the top back behind the nudge. The nudge clears only when a
/// snapshot arrives whose `exchange_timestamp_ns` is later than the
/// moment the nudge was made by more than `NUDGE_MIN_HOLD_NS`,
/// signalling that the server has had a chance to incorporate the
/// moved top into its outgoing snapshot stream (snapshot timestamps
/// are stamped at publish time and can postdate the book state they
/// carry — see `NUDGE_MIN_HOLD_NS`). There is no expiry TTL — the
/// only auto-clear is via this timestamp-plus-margin signal.
pub struct OrderbookManager {
    books: HashMap<String, OrderBookSnapshot>,
    bid_nudges: HashMap<String, Nudge>,
    ask_nudges: HashMap<String, Nudge>,
}

impl OrderbookManager {
    pub fn new() -> Self {
        Self {
            books: HashMap::new(),
            bid_nudges: HashMap::new(),
            ask_nudges: HashMap::new(),
        }
    }

    /// Update cache with a new orderbook snapshot. Auto-clears nudges
    /// once this snapshot's `exchange_timestamp_ns` is past the nudge
    /// moment by more than `NUDGE_MIN_HOLD_NS` — i.e. the server
    /// generated this snapshot long enough after our nudge moment that
    /// it must reflect the post-move book, so it's authoritative.
    pub fn update(&mut self, ob: &OrderBookSnapshot) {
        // Compare nudge.created_ns (local wall-clock) against
        // ob.exchange_timestamp_ns (server wall-clock). Both are
        // Unix-epoch ns. The NUDGE_MIN_HOLD_NS margin covers both the
        // publisher pipeline lag (snapshots stamped after the nudge can
        // still carry the pre-move book) and modest NTP drift between
        // the two clocks.
        if let Some(n) = self.bid_nudges.get(&ob.symbol).copied() {
            if ob.exchange_timestamp_ns > n.created_ns + NUDGE_MIN_HOLD_NS {
                self.bid_nudges.remove(&ob.symbol);
            }
        }
        if let Some(n) = self.ask_nudges.get(&ob.symbol).copied() {
            if ob.exchange_timestamp_ns > n.created_ns + NUDGE_MIN_HOLD_NS {
                self.ask_nudges.remove(&ob.symbol);
            }
        }

        // Reuse existing entry to avoid symbol String clone on every update.
        if let Some(existing) = self.books.get_mut(&ob.symbol) {
            existing.exchange = ob.exchange;
            existing.bids.clone_from(&ob.bids);
            existing.asks.clone_from(&ob.asks);
            existing.exchange_timestamp_ns = ob.exchange_timestamp_ns;
            existing.local_timestamp_ns = ob.local_timestamp_ns;
        } else {
            self.books.insert(ob.symbol.clone(), ob.clone());
        }
    }

    /// Get the latest orderbook for a symbol.
    pub fn get(&self, symbol: &str) -> Option<&OrderBookSnapshot> {
        self.books.get(symbol)
    }

    /// Get best bid price for a symbol — clamped up by an active
    /// Sell-side nudge if present.
    pub fn best_bid_price(&self, symbol: &str) -> Option<f64> {
        let book_bid = self.books.get(symbol).and_then(|b| b.best_bid()).map(|l| l.price);
        let nudge = self.bid_nudges.get(symbol).map(|n| n.price);
        match (book_bid, nudge) {
            (Some(b), Some(n)) => Some(b.max(n)),
            (Some(b), None)    => Some(b),
            (None,    Some(n)) => Some(n),
            (None,    None)    => None,
        }
    }

    /// Get best ask price for a symbol — clamped down by an active
    /// Buy-side nudge if present.
    pub fn best_ask_price(&self, symbol: &str) -> Option<f64> {
        let book_ask = self.books.get(symbol).and_then(|b| b.best_ask()).map(|l| l.price);
        let nudge = self.ask_nudges.get(symbol).map(|n| n.price);
        match (book_ask, nudge) {
            (Some(b), Some(n)) => Some(b.min(n)),
            (Some(b), None)    => Some(b),
            (None,    Some(n)) => Some(n),
            (None,    None)    => None,
        }
    }

    /// Get mid price for a symbol.
    pub fn mid_price(&self, symbol: &str) -> Option<f64> {
        if let Some(book) = self.books.get(symbol) {
            Some(book.mid_price())
        } else {
            None
        }
    }

    /// Get spread for a symbol.
    pub fn spread(&self, symbol: &str) -> Option<f64> {
        self.books.get(symbol)?.spread()
    }

    /// All cached orderbooks.
    pub fn books(&self) -> &HashMap<String, OrderBookSnapshot> {
        &self.books
    }

    /// Apply an inferred top-of-book bound from a server rejection
    /// (e.g. Polymarket "post-only crosses book" implies the real
    /// bid ≥ our SELL price). Stored as a timestamp-anchored nudge
    /// in `bid_nudges` / `ask_nudges`; `best_bid_price` /
    /// `best_ask_price` max/min the book against the nudge.
    ///
    /// Why timestamp-anchored override instead of mutating
    /// `book.bids`/`book.asks`: the previous design pushed a synthetic
    /// level into the book vec, which got wiped on the very next WS
    /// snapshot — and snapshots arrive every 50-200 ms during active
    /// markets, often still showing the pre-move book (the snapshot
    /// was generated server-side BEFORE our reject revealed the real
    /// top). So the strategy would re-emit the same crossing post-only
    /// price 3-10× before the snapshot eventually caught up. Observed
    /// 2026-04-29 04:22:15 (coid 1777433120347-...363, ~17 consecutive
    /// same-price post-only rejects on the same token).
    ///
    /// Auto-clear: nudges clear only when an incoming snapshot's
    /// `exchange_timestamp_ns` exceeds the nudge's `created_ns` by more
    /// than `NUDGE_MIN_HOLD_NS` (see `update`). There is no expiry
    /// TTL — a nudge stays alive as long as the server hasn't yet
    /// produced a snapshot comfortably newer than the moment we made
    /// it.
    ///
    /// Semantics:
    ///   * `side == Sell`: real bid ≥ price → set bid_nudge → best_bid_price
    ///     returns max(book bid, nudge price).
    ///   * `side == Buy`:  real ask ≤ price → set ask_nudge → best_ask_price
    ///     returns min(book ask, nudge price).
    ///
    /// Returns `true` iff the nudge actually tightened the bound
    /// (current view didn't already satisfy the assertion).
    pub fn nudge_inferred_top(
        &mut self,
        symbol: &str,
        side: crate::types::Side,
        price: f64,
        now_ns: u64,
    ) -> bool {
        match side {
            crate::types::Side::Sell => {
                // Caller asserts real bid ≥ price.
                let book_bid = self.books.get(symbol).and_then(|b| b.best_bid()).map(|l| l.price)
                    .unwrap_or(0.0);
                let cur_nudge = self.bid_nudges.get(symbol).copied();
                let cur = match cur_nudge {
                    Some(n) => book_bid.max(n.price),
                    None    => book_bid,
                };
                if price > cur {
                    self.bid_nudges.insert(symbol.to_string(),
                        Nudge { price, created_ns: now_ns });
                    return true;
                }
            }
            crate::types::Side::Buy => {
                // Caller asserts real ask ≤ price.
                let book_ask = self.books.get(symbol).and_then(|b| b.best_ask()).map(|l| l.price)
                    .unwrap_or(f64::INFINITY);
                let cur_nudge = self.ask_nudges.get(symbol).copied();
                let cur = match cur_nudge {
                    Some(n) => book_ask.min(n.price),
                    None    => book_ask,
                };
                if price < cur {
                    self.ask_nudges.insert(symbol.to_string(),
                        Nudge { price, created_ns: now_ns });
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Exchange, PriceLevel, Side};

    fn empty_book(symbol: &str) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: symbol.to_string(),
            bids: vec![],
            asks: vec![],
            exchange_timestamp_ns: 0,
            local_timestamp_ns: 0,
        }
    }

    /// Helper: real wall-clock timestamp.
    fn now() -> u64 { crate::types::now_ns() }

    #[test]
    fn nudge_bid_up_on_postonly_sell_reject() {
        // Pre-existing book with bid=0.55. Strategy SELL @ 0.57 gets
        // post-only-rejected → server bid is actually ≥ 0.57. Nudge
        // bumps cached bid up to 0.57.
        let mut om = OrderbookManager::new();
        let mut book = empty_book("tok");
        book.bids = vec![PriceLevel { price: 0.55, quantity: 10.0 }];
        book.asks = vec![PriceLevel { price: 0.62, quantity: 10.0 }];
        om.update(&book);

        let nudged = om.nudge_inferred_top("tok", Side::Sell, 0.57, now());
        assert!(nudged);
        assert_eq!(om.best_bid_price("tok"), Some(0.57));
        // Asks untouched.
        assert_eq!(om.best_ask_price("tok"), Some(0.62));
    }

    #[test]
    fn nudge_ask_down_on_postonly_buy_reject() {
        let mut om = OrderbookManager::new();
        let mut book = empty_book("tok");
        book.bids = vec![PriceLevel { price: 0.45, quantity: 10.0 }];
        book.asks = vec![PriceLevel { price: 0.50, quantity: 10.0 }];
        om.update(&book);

        // BUY @ 0.43 rejected → real ask ≤ 0.43.
        let nudged = om.nudge_inferred_top("tok", Side::Buy, 0.43, now());
        assert!(nudged);
        assert_eq!(om.best_ask_price("tok"), Some(0.43));
        assert_eq!(om.best_bid_price("tok"), Some(0.45));
    }

    #[test]
    fn nudge_no_op_when_cache_already_matches_inference() {
        let mut om = OrderbookManager::new();
        let mut book = empty_book("tok");
        book.bids = vec![PriceLevel { price: 0.60, quantity: 10.0 }];
        om.update(&book);

        // SELL @ 0.57 reject implies real bid ≥ 0.57. But cache
        // already has 0.60 ≥ 0.57 — no work to do.
        let nudged = om.nudge_inferred_top("tok", Side::Sell, 0.57, now());
        assert!(!nudged);
        assert_eq!(om.best_bid_price("tok"), Some(0.60));
    }

    #[test]
    fn nudge_creates_synthetic_book_when_no_snapshot_yet() {
        let mut om = OrderbookManager::new();
        // No prior snapshot for this token.
        let nudged = om.nudge_inferred_top("tok", Side::Sell, 0.57, now());
        assert!(nudged);
        assert_eq!(om.best_bid_price("tok"), Some(0.57));
        assert_eq!(om.best_ask_price("tok"), None);
    }

    #[test]
    fn nudge_survives_stale_ws_snapshot() {
        // Reproduces 2026-04-29 04:22:15 bug: SELL @ 0.69 rejected,
        // nudge bid to 0.69, but a stale WS snapshot (still showing
        // pre-move bid=0.65) arrives 30 ms later. Pre-fix: nudge is
        // wiped, next quote tick re-emits SELL @ 0.69 post_only=true,
        // looping forever. Post-fix: nudge survives because the
        // stale snapshot's exchange_timestamp_ns is BEFORE the
        // nudge's created_ns.
        let mut om = OrderbookManager::new();
        let nudge_ts = 1_000_000_000_000u64;       // arbitrary anchor
        let stale_server_ts = nudge_ts - 50_000_000; // 50 ms before nudge

        let mut book = empty_book("tok");
        book.bids = vec![PriceLevel { price: 0.65, quantity: 10.0 }];
        book.asks = vec![PriceLevel { price: 0.72, quantity: 10.0 }];
        book.exchange_timestamp_ns = stale_server_ts;
        om.update(&book);

        let _ = om.nudge_inferred_top("tok", Side::Sell, 0.69, nudge_ts);
        assert_eq!(om.best_bid_price("tok"), Some(0.69));

        // Stale WS snapshot: server-side ts is BEFORE the nudge moment,
        // so it must not clear the nudge.
        let mut stale = empty_book("tok");
        stale.bids = vec![PriceLevel { price: 0.65, quantity: 8.0 }];
        stale.asks = vec![PriceLevel { price: 0.72, quantity: 8.0 }];
        stale.exchange_timestamp_ns = stale_server_ts;
        om.update(&stale);

        // Nudge still authoritative.
        assert_eq!(om.best_bid_price("tok"), Some(0.69));
        assert_eq!(om.best_ask_price("tok"), Some(0.72));
    }

    #[test]
    fn nudge_clears_only_after_min_hold_elapsed() {
        // The clear condition is `exchange_timestamp_ns > created_ns +
        // NUDGE_MIN_HOLD_NS`. Snapshots stamped after the nudge but
        // within the hold margin are treated as possibly pre-move
        // (publisher pipeline lag) and keep the nudge alive.
        let mut om = OrderbookManager::new();
        let nudge_ts = 1_000_000_000_000u64;

        let mut book = empty_book("tok");
        book.bids = vec![PriceLevel { price: 0.65, quantity: 10.0 }];
        book.exchange_timestamp_ns = nudge_ts - 100_000_000;
        om.update(&book);

        let _ = om.nudge_inferred_top("tok", Side::Sell, 0.69, nudge_ts);

        // Snapshot ts equal to nudge ts → nudge survives.
        let mut equal_ts = empty_book("tok");
        equal_ts.bids = vec![PriceLevel { price: 0.66, quantity: 5.0 }];
        equal_ts.exchange_timestamp_ns = nudge_ts;
        om.update(&equal_ts);
        assert_eq!(om.best_bid_price("tok"), Some(0.69));

        // Snapshot ts after the nudge but exactly at the hold boundary
        // → not strictly beyond the margin → nudge survives.
        let mut at_hold = empty_book("tok");
        at_hold.bids = vec![PriceLevel { price: 0.66, quantity: 5.0 }];
        at_hold.exchange_timestamp_ns = nudge_ts + NUDGE_MIN_HOLD_NS;
        om.update(&at_hold);
        assert_eq!(om.best_bid_price("tok"), Some(0.69));

        // Snapshot ts strictly beyond the hold margin → clears.
        let mut after = empty_book("tok");
        after.bids = vec![PriceLevel { price: 0.70, quantity: 5.0 }];
        after.exchange_timestamp_ns = nudge_ts + NUDGE_MIN_HOLD_NS + 1;
        om.update(&after);
        assert_eq!(om.best_bid_price("tok"), Some(0.70));
    }

    #[test]
    fn nudge_survives_pseudo_fresh_snapshot_within_hold() {
        // Reproduces 2026-07-23 08:02:01 live: SELL @ 0.41 post-only
        // rejected → nudge bid to 0.41. Polymarket then delivered
        // snapshots stamped 60-100 ms AFTER the nudge moment that still
        // carried the pre-move bid (publisher stamps at emit time, not
        // book-state time). Pre-fix the bare `ts > created_ns` check
        // cleared the nudge on each one, and the same crossing price
        // was re-emitted and rejected 4× in 300 ms. Post-fix those
        // pseudo-fresh snapshots fall inside NUDGE_MIN_HOLD_NS and the
        // nudge holds.
        let mut om = OrderbookManager::new();
        let nudge_ts = 1_000_000_000_000u64;

        let mut book = empty_book("tok");
        book.bids = vec![PriceLevel { price: 0.40, quantity: 10.0 }];
        book.exchange_timestamp_ns = nudge_ts - 30_000_000;
        om.update(&book);

        let _ = om.nudge_inferred_top("tok", Side::Sell, 0.41, nudge_ts);
        assert_eq!(om.best_bid_price("tok"), Some(0.41));

        // Three pseudo-fresh stale snapshots, ~100 ms apart, all within
        // the hold margin — none may clear the nudge.
        for lag_ms in [60u64, 160, 260] {
            let mut pseudo = empty_book("tok");
            pseudo.bids = vec![PriceLevel { price: 0.40, quantity: 10.0 }];
            pseudo.exchange_timestamp_ns = nudge_ts + lag_ms * 1_000_000;
            om.update(&pseudo);
            assert_eq!(om.best_bid_price("tok"), Some(0.41),
                "nudge cleared by pseudo-fresh snapshot at +{} ms", lag_ms);
        }
    }

    #[test]
    fn nudge_clears_then_subsequent_lower_bid_honoured() {
        // Once the server has produced a snapshot comfortably past the
        // nudge (beyond the hold margin), the nudge is gone —
        // subsequent snapshots can legitimately lower the bid (e.g.
        // price moved up briefly then dropped back).
        let mut om = OrderbookManager::new();
        let nudge_ts = 1_000_000_000_000u64;

        let mut book = empty_book("tok");
        book.bids = vec![PriceLevel { price: 0.65, quantity: 10.0 }];
        book.exchange_timestamp_ns = nudge_ts - 100_000_000;
        om.update(&book);

        let _ = om.nudge_inferred_top("tok", Side::Sell, 0.69, nudge_ts);

        // Note the interim 0.70 print while the nudge is still held:
        // best_bid = max(book 0.70, nudge 0.69) = 0.70, so a higher
        // book top shows through even before the nudge clears.
        let mut confirm = empty_book("tok");
        confirm.bids = vec![PriceLevel { price: 0.70, quantity: 8.0 }];
        confirm.exchange_timestamp_ns = nudge_ts + 50_000_000;
        om.update(&confirm);
        assert_eq!(om.best_bid_price("tok"), Some(0.70));

        // Server confirms post-move state beyond the hold margin.
        let mut past_hold = empty_book("tok");
        past_hold.bids = vec![PriceLevel { price: 0.70, quantity: 8.0 }];
        past_hold.exchange_timestamp_ns = nudge_ts + NUDGE_MIN_HOLD_NS + 100_000_000;
        om.update(&past_hold);
        assert_eq!(om.best_bid_price("tok"), Some(0.70));

        // Later snapshot lowers the bid — should be honoured (nudge gone).
        let mut later = empty_book("tok");
        later.bids = vec![PriceLevel { price: 0.66, quantity: 5.0 }];
        later.exchange_timestamp_ns = nudge_ts + NUDGE_MIN_HOLD_NS + 200_000_000;
        om.update(&later);
        assert_eq!(om.best_bid_price("tok"), Some(0.66));
    }

    #[test]
    fn no_ttl_nudge_persists_indefinitely_until_server_catches_up() {
        // No fixed TTL: as long as no WS snapshot is fresher than the
        // nudge, the nudge stays alive — even across many stale
        // snapshots. Documents that the auto-clear is purely
        // server-timestamp-driven.
        let mut om = OrderbookManager::new();
        let nudge_ts = 1_000_000_000_000u64;

        let mut book = empty_book("tok");
        book.bids = vec![PriceLevel { price: 0.65, quantity: 10.0 }];
        book.exchange_timestamp_ns = nudge_ts - 100_000_000;
        om.update(&book);
        let _ = om.nudge_inferred_top("tok", Side::Sell, 0.69, nudge_ts);

        // 10 stale snapshots in a row, all server-side BEFORE the nudge.
        for i in 1..=10 {
            let mut stale = empty_book("tok");
            stale.bids = vec![PriceLevel { price: 0.65, quantity: 10.0 }];
            stale.exchange_timestamp_ns = nudge_ts - 100_000_000 + i;
            om.update(&stale);
            assert_eq!(om.best_bid_price("tok"), Some(0.69),
                "nudge dropped early at iteration {}", i);
        }
    }
}
