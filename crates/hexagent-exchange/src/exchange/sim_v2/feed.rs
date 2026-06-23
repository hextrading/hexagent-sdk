//! Server-axis market feed for sim_v2.
//!
//! Wraps the Polymarket `MarketReplayer`s and emits book/trade/instrument
//! events on the SERVER time axis. Books carry a real `exchange_timestamp_ns`;
//! trades do not (the recorder stamps their `exchange_timestamp_ns` with the
//! local receive time, `market.rs`), so we reconstruct a server time by
//! anchoring each trade to the most recent book for the same token:
//!
//! ```text
//! t_srv = anchor.book_srv + max(0, trade.local − anchor.book_local)
//! t_srv = max(t_srv, last_srv[token] + 1)        // strict monotonic
//! ```
//!
//! TCP single-connection ordering means the replayer's native (local) order
//! equals the server send order, so a streaming anchor is correct. The
//! `Scheduler` re-sorts by `t_srv` anyway.

use std::collections::{HashMap, VecDeque};
use std::path::Path;

/// Keep trade-reconstruction state (`anchors`/`last_srv`) for at most this many
/// recently-seen tokens. A settled event's tokens stop appearing in the feed, so
/// evicting the oldest beyond this window is result-neutral (their anchor is
/// never read again) — it just bounds memory over long runs. Far above the few
/// tokens live at any instant.
const FEED_TOKEN_CAP: usize = 128;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::recorder::MarketReplayer;
use crate::types::MarketEvent;

use super::event::SimEvent;

/// Most recent book seen for a token: (server ts, local ts).
type Anchor = (u64, u64);

/// Pure reconstruction of a trade's server timestamp. Returns
/// `(t_srv, anchored)` where `anchored` is false when there was no prior book
/// (fallback to the recorded local-receive ts == v1 behavior).
fn reconstruct_trade_srv(
    anchor: Option<Anchor>,
    last_srv: Option<u64>,
    trade_local_ns: u64,
    trade_exch_ns: u64,
) -> (u64, bool) {
    let (raw, anchored) = match anchor {
        Some((book_srv, book_local)) => {
            let delta = (trade_local_ns as i128 - book_local as i128).max(0) as u64;
            (book_srv.saturating_add(delta), true)
        }
        None => (trade_exch_ns, false),
    };
    let floored = match last_srv {
        Some(l) => raw.max(l + 1),
        None => raw,
    };
    (floored, anchored)
}

pub struct ServerFeed {
    replayers: Vec<MarketReplayer>,
    /// One reconstructed lookahead per replayer.
    peeked: Vec<Option<(u64, SimEvent)>>,
    anchors: HashMap<String, Anchor>,
    last_srv: HashMap<String, u64>,
    /// Insertion order of distinct tokens in `anchors`/`last_srv`, for FIFO
    /// eviction past `FEED_TOKEN_CAP` (bounds memory over long runs).
    token_order: VecDeque<String>,
    anchored_trades: u64,
    fallback_trades: u64,
}

impl ServerFeed {
    /// Build from the Polymarket `(exchange, symbol)` sources. Non-polymarket
    /// sources are ignored (the sim only matches Polymarket). Sources whose
    /// files are absent in range are skipped, mirroring v1.
    pub fn new(
        data_dir: &Path,
        sources: &[(String, String)],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Self> {
        let mut replayers = Vec::new();
        for (exchange, symbol) in sources {
            if exchange != "polymarket" {
                continue;
            }
            if let Ok(r) = MarketReplayer::new(data_dir, exchange, symbol, start, end) {
                replayers.push(r);
            }
        }
        let mut feed = Self {
            peeked: vec![None; replayers.len()],
            replayers,
            anchors: HashMap::new(),
            last_srv: HashMap::new(),
            token_order: VecDeque::new(),
            anchored_trades: 0,
            fallback_trades: 0,
        };
        for i in 0..feed.replayers.len() {
            feed.refill(i);
        }
        Ok(feed)
    }

    /// Record a token's first appearance and FIFO-evict trade-reconstruction
    /// state past `FEED_TOKEN_CAP`. Only evicts the oldest (long-dead) token, so
    /// the current token's anchor is never touched → result-neutral.
    fn track_token(&mut self, token: &str) {
        if self.last_srv.contains_key(token) {
            return; // already tracked
        }
        self.token_order.push_back(token.to_string());
        while self.token_order.len() > FEED_TOKEN_CAP {
            if let Some(old) = self.token_order.pop_front() {
                self.anchors.remove(&old);
                self.last_srv.remove(&old);
            }
        }
    }

    /// Pull the next relevant event from replayer `i`, reconstruct its server
    /// time, update anchor/monotonic state, and store it in `peeked[i]`.
    /// Skips events the matching core doesn't consume (quote/spot/etc.).
    fn refill(&mut self, i: usize) {
        loop {
            let next = self.replayers[i].next_event().ok().flatten();
            let Some((local_ts, event)) = next else {
                self.peeked[i] = None;
                return;
            };
            match event {
                MarketEvent::OrderBook(ob) => {
                    let token = ob.symbol.clone();
                    self.track_token(&token);
                    self.anchors
                        .insert(token.clone(), (ob.exchange_timestamp_ns, ob.local_timestamp_ns));
                    let floor = self
                        .last_srv
                        .get(&token)
                        .copied()
                        .unwrap_or(0)
                        .max(ob.exchange_timestamp_ns);
                    self.last_srv.insert(token, floor);
                    self.peeked[i] = Some((ob.exchange_timestamp_ns, SimEvent::ServerBook(ob)));
                    return;
                }
                MarketEvent::Trade(mut t) => {
                    let token = t.symbol.clone();
                    self.track_token(&token);
                    let (t_srv, anchored) = reconstruct_trade_srv(
                        self.anchors.get(&token).copied(),
                        self.last_srv.get(&token).copied(),
                        t.local_timestamp_ns,
                        t.exchange_timestamp_ns,
                    );
                    if anchored {
                        self.anchored_trades += 1;
                    } else {
                        self.fallback_trades += 1;
                    }
                    self.last_srv.insert(token, t_srv);
                    t.exchange_timestamp_ns = t_srv;
                    self.peeked[i] = Some((t_srv, SimEvent::ServerTrade(t)));
                    return;
                }
                MarketEvent::Instrument(inst) => {
                    self.peeked[i] = Some((local_ts, SimEvent::ServerInstrument(inst)));
                    return;
                }
                MarketEvent::TickSizeChange(tsc) => {
                    self.peeked[i] = Some((tsc.local_timestamp_ns, SimEvent::ServerTickSize(tsc)));
                    return;
                }
                // Quote / SpotPrice / Connected / Disconnected / EventStart /
                // Bar / Exit — not consumed by the matching core; skip.
                _ => continue,
            }
        }
    }

    /// Wall-clock (server) time of the next event across all replayers.
    pub fn peek_when(&self) -> Option<u64> {
        self.peeked
            .iter()
            .filter_map(|p| p.as_ref().map(|(ts, _)| *ts))
            .min()
    }

    /// Pop the earliest server-axis event (k-way merge by reconstructed ts).
    pub fn next_server_event(&mut self) -> Option<(u64, SimEvent)> {
        let mut best_idx = None;
        let mut best_ts = u64::MAX;
        for (i, p) in self.peeked.iter().enumerate() {
            if let Some((ts, _)) = p {
                if *ts < best_ts {
                    best_ts = *ts;
                    best_idx = Some(i);
                }
            }
        }
        let i = best_idx?;
        let out = self.peeked[i].take();
        self.refill(i);
        out
    }

    /// (anchored, fallback) trade counts — fallback flags anchor staleness.
    pub fn trade_anchor_stats(&self) -> (u64, u64) {
        (self.anchored_trades, self.fallback_trades)
    }

    /// One-step lookahead for the sim_v2 "race" model: the next book snapshot
    /// for `token` strictly after server time `after_ns`. The immediately-peeked
    /// event (already pulled out of the replayer's rows) is checked first, then
    /// the unconsumed rows are scanned. Returns the next book's `(bids, asks)`.
    pub fn peek_next_book(
        &self,
        token: &str,
        after_ns: u64,
    ) -> Option<(u64, Vec<crate::types::PriceLevel>, Vec<crate::types::PriceLevel>)> {
        for p in &self.peeked {
            if let Some((ts, SimEvent::ServerBook(ob))) = p {
                if ob.symbol == token && *ts > after_ns {
                    return Some((*ts, ob.bids.clone(), ob.asks.clone()));
                }
            }
        }
        for r in &self.replayers {
            if let Some(x) = r.peek_next_book(token, after_ns) {
                return Some(x);
            }
        }
        None
    }

    /// Like [`peek_next_book`] but returns BORROWED level slices (no clone) —
    /// for read-only callers (the forward-markout mid peek). Same selection.
    pub fn peek_next_book_ref(
        &self,
        token: &str,
        after_ns: u64,
    ) -> Option<(u64, &[crate::types::PriceLevel], &[crate::types::PriceLevel])> {
        for p in &self.peeked {
            if let Some((ts, SimEvent::ServerBook(ob))) = p {
                if ob.symbol == token && *ts > after_ns {
                    return Some((*ts, &ob.bids, &ob.asks));
                }
            }
        }
        for r in &self.replayers {
            if let Some(x) = r.peek_next_book_ref(token, after_ns) {
                return Some(x);
            }
        }
        None
    }

    /// All book snapshots for `token` in `(after_ns, until_ns]` (taker windowed
    /// race). Includes the immediately-peeked event if it falls in the window,
    /// then the unconsumed rows.
    pub fn peek_books_in_window(
        &self,
        token: &str,
        after_ns: u64,
        until_ns: u64,
    ) -> Vec<(u64, Vec<crate::types::PriceLevel>, Vec<crate::types::PriceLevel>)> {
        let mut out = Vec::new();
        for p in &self.peeked {
            if let Some((ts, SimEvent::ServerBook(ob))) = p {
                if ob.symbol == token && *ts > after_ns && *ts <= until_ns {
                    out.push((*ts, ob.bids.clone(), ob.asks.clone()));
                }
            }
        }
        for r in &self.replayers {
            out.extend(r.peek_books_in_window(token, after_ns, until_ns));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconstruct_basic() {
        // book@(srv=1000, local=500), trade@(local=520) → 1000 + 20 = 1020.
        let (t, anchored) = reconstruct_trade_srv(Some((1000, 500)), Some(1000), 520, 520);
        assert_eq!(t, 1020);
        assert!(anchored);
    }

    #[test]
    fn reconstruct_two_trades_same_anchor_clamp() {
        // First trade → 1020.
        let (t1, _) = reconstruct_trade_srv(Some((1000, 500)), Some(1000), 520, 520);
        assert_eq!(t1, 1020);
        // Second trade with identical local ts → must strictly exceed last_srv.
        let (t2, _) = reconstruct_trade_srv(Some((1000, 500)), Some(t1), 520, 520);
        assert_eq!(t2, 1021);
    }

    #[test]
    fn reconstruct_no_prior_book_fallback() {
        // No anchor → fall back to recorded exchange (== local) ts; not anchored.
        let (t, anchored) = reconstruct_trade_srv(None, None, 777, 777);
        assert_eq!(t, 777);
        assert!(!anchored);
    }

    #[test]
    fn reconstruct_monotonic_clamp_on_backwards_local() {
        // trade local < anchor book local → delta clamped to 0, then floored.
        let (t, _) = reconstruct_trade_srv(Some((2000, 900)), Some(2000), 850, 850);
        // raw = 2000 + 0 = 2000; floor = max(2000, 2000+1) = 2001.
        assert_eq!(t, 2001);
    }

    #[test]
    fn reconstruct_per_token_independence_via_state() {
        // Token A anchored, token B unanchored — handled independently because
        // the caller keys anchors/last_srv by token. Verify the pure fn honors
        // whatever per-token state it's handed.
        let (ta, a_anchored) = reconstruct_trade_srv(Some((5000, 1000)), Some(5000), 1100, 1100);
        assert_eq!(ta, 5100);
        assert!(a_anchored);
        let (tb, b_anchored) = reconstruct_trade_srv(None, None, 1100, 1100);
        assert_eq!(tb, 1100);
        assert!(!b_anchored);
    }
}
