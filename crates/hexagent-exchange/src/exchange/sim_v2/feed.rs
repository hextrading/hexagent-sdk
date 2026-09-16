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
//! legacy: t_srv = max(t_srv, last_srv[token] + 1)
//! strict: apply = max(last_apply[stream], reconstructed_source)
//! ```
//!
//! Native replay order is retained within each source. The tape has no venue
//! sequence/connection session, so it cannot prove TCP continuity or establish
//! the order of separate connections. Equal timestamps remain legal distinct
//! updates; source index then native sequence give deterministic replay ties.

use std::collections::{HashMap, VecDeque};
use std::path::Path;

/// Keep trade-reconstruction state (`anchors`/`last_srv`) for at most this many
/// recently-seen tokens. A settled event's tokens stop appearing in the feed, so
/// evicting the oldest beyond this window is result-neutral (their anchor is
/// never read again) — it just bounds memory over long runs. Far above the few
/// tokens live at any instant.
const FEED_TOKEN_CAP: usize = 128;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::recorder::{MarketReplayer, ReplayOptions, ReplayTimePolicy};
use crate::types::MarketEvent;

use super::event::{ServerEventTime, SimEvent};

/// Most recent book seen for a token: (server ts, local ts).
type Anchor = (u64, u64);

/// Strict source-time anchors are independent between native streams. One
/// stream cannot change another's inferred public-trade clock. Sole writer is
/// ServerFeed; each map is preallocated and FIFO-bounded at FEED_TOKEN_CAP.
struct SourceAnchors {
    books: HashMap<String, Anchor>,
    order: VecDeque<String>,
}

impl SourceAnchors {
    fn new() -> Self {
        Self {
            books: HashMap::with_capacity(FEED_TOKEN_CAP),
            order: VecDeque::with_capacity(FEED_TOKEN_CAP),
        }
    }

    fn observe_book(&mut self, token: &str, source_ns: u64, receive_ns: u64) {
        if let Some(anchor) = self.books.get_mut(token) {
            // Equal source ns can be a legal distinct update. Do not turn a
            // timestamp into a dedup key; only a strictly older book is stale.
            if source_ns >= anchor.0 {
                *anchor = (source_ns, receive_ns);
            }
            return;
        }
        if self.books.len() == FEED_TOKEN_CAP {
            if let Some(old) = self.order.pop_front() {
                self.books.remove(&old);
            }
        }
        self.order.push_back(token.to_string());
        self.books
            .insert(token.to_string(), (source_ns, receive_ns));
    }
}

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
        Some(l) => raw.max(l.saturating_add(1)),
        None => raw,
    };
    (floored, anchored)
}

/// Keep one replay stream's server lane monotonic without borrowing the
/// strategy/local clock. Venue timestamps occasionally move backwards by a
/// few milliseconds in an otherwise ordered websocket stream; replay must
/// preserve native stream order instead of rewinding the matching engine.
#[inline]
fn monotonic_server_time(last_ns: u64, raw_ns: u64) -> (u64, u64) {
    (last_ns.max(raw_ns), last_ns.saturating_sub(raw_ns))
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
    /// Last effective server-lane timestamp per native replay stream. This is
    /// deliberately independent of every recorded local/strategy timestamp.
    last_replayer_srv: Vec<u64>,
    server_time_regressions: u64,
    max_server_time_regression_ns: u64,
    raw_server_clock: bool,
    source_anchors: Vec<SourceAnchors>,
    stream_sequence: Vec<u64>,
    strict_reader_errors: bool,
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
        Self::new_with_replay_options(data_dir, sources, start, end, ReplayOptions::default())
    }

    pub fn new_with_replay_options(
        data_dir: &Path,
        sources: &[(String, String)],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        replay_options: ReplayOptions,
    ) -> Result<Self> {
        Self::new_with_clock_options(data_dir, sources, start, end, replay_options, false)
    }

    pub fn new_with_clock_options(
        data_dir: &Path,
        sources: &[(String, String)],
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        replay_options: ReplayOptions,
        raw_server_clock: bool,
    ) -> Result<Self> {
        anyhow::ensure!(
            replay_options.time_policy != ReplayTimePolicy::ArrivalTimeStrict
                || !replay_options.bootstrap_binary_open,
            "ArrivalTimeStrict is incompatible with future binary-open bootstrap"
        );
        let strict_reader_errors =
            raw_server_clock || replay_options.time_policy == ReplayTimePolicy::ArrivalTimeStrict;
        let mut replayers = Vec::new();
        for (exchange, symbol) in sources {
            if exchange != "polymarket" {
                continue;
            }
            let result = MarketReplayer::new_with_options(
                data_dir,
                exchange,
                symbol,
                start,
                end,
                replay_options,
            );
            if strict_reader_errors {
                replayers.push(
                    result.with_context(|| format!("strict server source {exchange}/{symbol}"))?,
                );
            } else if let Ok(r) = result {
                replayers.push(r); // historical optional-source behavior
            }
        }
        let replayer_count = replayers.len();
        let mut feed = Self {
            peeked: vec![None; replayers.len()],
            replayers,
            anchors: HashMap::new(),
            last_srv: HashMap::new(),
            token_order: VecDeque::new(),
            anchored_trades: 0,
            fallback_trades: 0,
            last_replayer_srv: vec![0; replayer_count],
            server_time_regressions: 0,
            max_server_time_regression_ns: 0,
            raw_server_clock,
            source_anchors: (0..replayer_count).map(|_| SourceAnchors::new()).collect(),
            stream_sequence: vec![0; replayer_count],
            strict_reader_errors,
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
            let result = self.replayers[i].next_event();
            let next = if self.strict_reader_errors {
                result.unwrap_or_else(|error| {
                    panic!(
                        "strict server replay stream {i} failed at sequence {}: {error:#}",
                        self.stream_sequence[i]
                    )
                })
            } else {
                result.ok().flatten()
            };
            let Some((local_ts, event)) = next else {
                self.peeked[i] = None;
                return;
            };
            if let Some(prepared) = self.prepare_server_event(i, local_ts, event) {
                self.peeked[i] = Some(prepared);
                return;
            }
        }
    }

    /// This is the production reader -> server-feed boundary. Keep payload
    /// source/receive evidence immutable in strict mode; only the returned key
    /// schedules application. Test fixtures call this same boundary directly.
    fn prepare_server_event(
        &mut self,
        i: usize,
        recorded_row_ns: u64,
        event: MarketEvent,
    ) -> Option<(u64, SimEvent)> {
        self.stream_sequence[i] = self.stream_sequence[i]
            .checked_add(1)
            .expect("server feed sequence exhausted");
        let sequence = self.stream_sequence[i];
        let (raw_ns, receive_ns, estimate_ns) = match &event {
            MarketEvent::OrderBook(ob) => (ob.exchange_timestamp_ns, ob.local_timestamp_ns, None),
            MarketEvent::Trade(t) => {
                let (estimate, anchored) = if self.raw_server_clock {
                    // Do not feed the logical application floor back into the
                    // physical source estimate. Equal-source trades are legal.
                    reconstruct_trade_srv(
                        self.source_anchors[i].books.get(&t.symbol).copied(),
                        None,
                        t.local_timestamp_ns,
                        t.exchange_timestamp_ns,
                    )
                } else {
                    self.track_token(&t.symbol);
                    reconstruct_trade_srv(
                        self.anchors.get(&t.symbol).copied(),
                        self.last_srv.get(&t.symbol).copied(),
                        t.local_timestamp_ns,
                        t.exchange_timestamp_ns,
                    )
                };
                if anchored {
                    self.anchored_trades += 1;
                } else {
                    self.fallback_trades += 1;
                }
                (
                    t.exchange_timestamp_ns,
                    t.local_timestamp_ns,
                    Some(estimate),
                )
            }
            MarketEvent::Instrument(_) => (recorded_row_ns, recorded_row_ns, None),
            MarketEvent::TickSizeChange(tsc) => {
                (tsc.local_timestamp_ns, tsc.local_timestamp_ns, None)
            }
            _ => return None,
        };
        let source_ns = estimate_ns.unwrap_or(raw_ns);
        let (effective_ns, regression_ns) =
            monotonic_server_time(self.last_replayer_srv[i], source_ns);
        self.observe_server_time(i, effective_ns, regression_ns);
        let timing = ServerEventTime {
            raw_source_ns: raw_ns,
            recorded_receive_ns: receive_ns,
            reconstructed_source_ns: estimate_ns,
            effective_apply_ns: effective_ns,
            stream_index: u32::try_from(i).expect("too many replay streams"),
            stream_sequence: sequence,
        };
        let prepared = match event {
            MarketEvent::OrderBook(mut ob) => {
                if self.raw_server_clock {
                    self.source_anchors[i].observe_book(&ob.symbol, raw_ns, receive_ns);
                } else {
                    // Legacy profile retains its historical rewritten payload
                    // and anchor feedback so old result manifests still replay.
                    ob.exchange_timestamp_ns = effective_ns;
                    let token = ob.symbol.clone();
                    self.track_token(&token);
                    self.anchors
                        .insert(token.clone(), (effective_ns, receive_ns));
                    let floor = self
                        .last_srv
                        .get(&token)
                        .copied()
                        .unwrap_or(0)
                        .max(effective_ns);
                    self.last_srv.insert(token, floor);
                }
                SimEvent::ServerBook(ob, timing)
            }
            MarketEvent::Trade(mut t) => {
                if !self.raw_server_clock {
                    self.last_srv.insert(t.symbol.clone(), effective_ns);
                    t.exchange_timestamp_ns = effective_ns;
                }
                SimEvent::ServerTrade(t, timing)
            }
            MarketEvent::Instrument(inst) => SimEvent::ServerInstrument(inst, timing),
            MarketEvent::TickSizeChange(tsc) => SimEvent::ServerTickSize(tsc, timing),
            _ => unreachable!("non-server events returned before clock application"),
        };
        Some((effective_ns, prepared))
    }

    pub fn raw_server_clock(&self) -> bool {
        self.raw_server_clock
    }

    #[inline]
    fn observe_server_time(&mut self, i: usize, effective_ns: u64, regression_ns: u64) {
        self.last_replayer_srv[i] = effective_ns;
        if regression_ns > 0 {
            self.server_time_regressions = self.server_time_regressions.saturating_add(1);
            self.max_server_time_regression_ns =
                self.max_server_time_regression_ns.max(regression_ns);
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
        #[cfg(test)]
        if i >= self.replayers.len() {
            return out;
        }
        self.refill(i);
        out
    }

    #[cfg(test)]
    pub(crate) fn test_push_server_event(&mut self, when: u64, event: SimEvent) {
        assert!(
            self.replayers.is_empty(),
            "test-only injected feed must not mix real replayers"
        );
        self.peeked.push(Some((when, event)));
    }

    /// (anchored, fallback) trade counts — fallback flags anchor staleness.
    pub fn trade_anchor_stats(&self) -> (u64, u64) {
        (self.anchored_trades, self.fallback_trades)
    }

    /// `(regression_count, max_regression_ns)` for raw venue timestamps that
    /// were clamped on the independent monotonic server lane.
    pub fn server_time_regression_stats(&self) -> (u64, u64) {
        (
            self.server_time_regressions,
            self.max_server_time_regression_ns,
        )
    }

    /// One-step lookahead for the sim_v2 "race" model: the next book snapshot
    /// for `token` strictly after server time `after_ns`. The immediately-peeked
    /// event (already pulled out of the replayer's rows) is checked first, then
    /// the unconsumed rows are scanned. Returns the next book's `(bids, asks)`.
    pub fn peek_next_book(
        &self,
        token: &str,
        after_ns: u64,
    ) -> Option<(
        u64,
        Vec<crate::types::PriceLevel>,
        Vec<crate::types::PriceLevel>,
    )> {
        for p in &self.peeked {
            if let Some((ts, SimEvent::ServerBook(ob, _))) = p {
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
    ) -> Option<(
        u64,
        &[crate::types::PriceLevel],
        &[crate::types::PriceLevel],
    )> {
        for p in &self.peeked {
            if let Some((ts, SimEvent::ServerBook(ob, _))) = p {
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
    ) -> Vec<(
        u64,
        Vec<crate::types::PriceLevel>,
        Vec<crate::types::PriceLevel>,
    )> {
        let mut out = Vec::new();
        for p in &self.peeked {
            if let Some((ts, SimEvent::ServerBook(ob, _))) = p {
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

    use super::super::exchange::SimExchangeV2;
    use crate::types::{
        Exchange, Instrument, OrderBookSnapshot, OrderRequest, OrderStatus, OrderType, OrderUpdate,
        PriceLevel, Side, TradeTick,
    };

    fn fixture_feed(strict: bool, streams: usize) -> ServerFeed {
        let mut feed = ServerFeed::new(
            Path::new("/nonexistent"),
            &[],
            DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            DateTime::<Utc>::from_timestamp(1, 0).unwrap(),
        )
        .unwrap();
        feed.raw_server_clock = strict;
        feed.last_replayer_srv = vec![0; streams];
        feed.stream_sequence = vec![0; streams];
        feed.source_anchors = (0..streams).map(|_| SourceAnchors::new()).collect();
        feed
    }

    fn instrument() -> Instrument {
        Instrument::BinaryOption(crate::types::instrument::BinaryOption {
            exchange: Exchange::Polymarket,
            id: "e".into(),
            question: "q".into(),
            condition_id: "condition".into(),
            series_slug: "s".into(),
            slug: "s".into(),
            clob_token_ids: vec!["up".into(), "down".into()],
            outcomes: vec!["Up".into(), "Down".into()],
            outcome_prices: vec![],
            active: true,
            closed: false,
            volume: 0.0,
            liquidity: 0.0,
            tick_size: 0.01,
            order_min_size: 5.0,
            group_item_title: String::new(),
            event_start_time: String::new(),
            base_fee: 0,
            fee_exponent: 0.0,
            fee_rate: 0.0,
            fee_settlement: Default::default(),
        })
    }

    fn fixture_core(folded: bool) -> SimExchangeV2 {
        let mut core = SimExchangeV2::new(500_000_000, HashMap::new(), HashMap::new());
        core.set_fold_outcomes(folded);
        core.on_instrument(&instrument());
        core.configure_maker_order_audit(true);
        core
    }

    fn snapshot(token: &str, source: u64, receive: u64, ask: f64, bid_qty: f64) -> MarketEvent {
        MarketEvent::OrderBook(OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: token.into(),
            bids: vec![PriceLevel {
                price: 0.60,
                quantity: bid_qty,
            }],
            asks: vec![PriceLevel {
                price: ask,
                quantity: 80.0,
            }],
            exchange_timestamp_ns: source,
            local_timestamp_ns: receive,
        })
    }

    fn print(token: &str, receive: u64) -> MarketEvent {
        MarketEvent::Trade(TradeTick {
            exchange: Exchange::Polymarket,
            symbol: token.into(),
            exchange_trade_id: None,
            price: 0.90,
            quantity: 1.0,
            side: Side::Buy,
            exchange_timestamp_ns: receive,
            local_timestamp_ns: receive,
        })
    }

    fn bid(coid: &str, price: f64) -> OrderRequest {
        OrderRequest {
            client_order_id: coid.into(),
            exchange: Exchange::Polymarket,
            symbol: "up".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(price),
            quantity: 12.0,
            quote_trigger_exchange_timestamp_ns: 0,
            quote_trigger_local_timestamp_ns: 0,
            quote_event_id: String::new(),
            quote_trigger_source: crate::types::QuoteTriggerSource::Unknown,
            timestamp_ns: 0,
            instance_id: "owner".into(),
            fee_rate_bps: 0,
            post_only: true,
            reduce_only: false,
            outcome_label: String::new(),
            order_slot: Default::default(),
        }
    }

    fn apply(core: &mut SimExchangeV2, strict: bool, event: SimEvent) -> Vec<OrderUpdate> {
        match event {
            SimEvent::ServerBook(ob, timing) => {
                if strict {
                    core.on_orderbook_at(&ob, &timing, None)
                } else {
                    core.on_orderbook(&ob)
                }
            }
            SimEvent::ServerTrade(t, timing) => {
                if strict {
                    core.on_trade_tick_at(&t, &timing, None)
                } else {
                    core.on_trade_tick(&t)
                }
            }
            SimEvent::ServerInstrument(i, _) => {
                core.on_instrument(&i);
                vec![]
            }
            _ => vec![],
        }
    }

    #[test]
    fn strict_feed_core_order_drops_old_book_after_trade_without_queue_or_age_refresh() {
        for folded in [false, true] {
            let mut feed = fixture_feed(true, 1);
            let mut core = fixture_core(folded);
            core.configure_unexplained_depletion_execution(1.0);
            core.configure_book_through(1.0);
            let (_, first) = feed
                .prepare_server_event(0, 1000, snapshot("up", 100, 1000, 0.62, 50.0))
                .unwrap();
            apply(&mut core, true, first);
            assert_eq!(
                core.submit_order(&bid("resting", 0.60), 101).status,
                OrderStatus::Accepted
            );
            let (when, trade) = feed
                .prepare_server_event(0, 1020, print("up", 1020))
                .unwrap();
            assert_eq!(when, 120);
            let SimEvent::ServerTrade(ref payload, time) = trade else {
                panic!("trade expected")
            };
            assert_eq!(
                payload.exchange_timestamp_ns, 1020,
                "recorded trade field stays immutable"
            );
            assert_eq!(time.reconstructed_source_ns, Some(120));
            apply(&mut core, true, trade);
            let (when, old) = feed
                .prepare_server_event(0, 1030, snapshot("up", 90, 1030, 0.59, 5.0))
                .unwrap();
            assert_eq!(when, 120, "DES time cannot rewind");
            let SimEvent::ServerBook(ref payload, time) = old else {
                panic!("book expected")
            };
            assert_eq!(
                (
                    payload.exchange_timestamp_ns,
                    time.raw_source_ns,
                    time.recorded_receive_ns
                ),
                (90, 90, 1030)
            );
            assert!(
                apply(&mut core, true, old).is_empty(),
                "old depth cannot generate depletion fills"
            );
            assert_eq!(core.raw_older_books_dropped, 1);
            assert_eq!(core.maker_order_audit_rows()[0].q_ahead_final, 50.0);
            assert_eq!(
                core.submit_order(&bid("post-only", 0.61), 120).status,
                OrderStatus::Accepted,
                "admission must use the accepted .62 ask, not rejected old .59 ask"
            );

            core.configure_book_stale_gate(15);
            let MarketEvent::OrderBook(local) = snapshot("up", 100, 120, 0.62, 50.0) else {
                unreachable!()
            };
            core.on_local_orderbook(&local, 120);
            core.submit_order(&bid("stale-source", 0.59), 125);
            assert_eq!(
                core.book_stale_exchange_hits, 1,
                "raw source age is 25, not 5"
            );
            assert_eq!(
                core.book_stale_local_hits, 0,
                "local receipt remains independent"
            );
            let (_, next_trade) = feed
                .prepare_server_event(0, 1040, print("up", 1040))
                .unwrap();
            let SimEvent::ServerTrade(_, time) = next_trade else {
                unreachable!()
            };
            assert_eq!(
                time.reconstructed_source_ns,
                Some(140),
                "old book must not replace source anchor"
            );
        }
    }

    #[test]
    fn strict_equal_source_books_and_duplicate_rows_are_not_timestamp_deduplicated() {
        let mut feed = fixture_feed(true, 1);
        let mut core = fixture_core(true);
        for (receive, ask) in [(1000, 0.62), (1010, 0.60), (1010, 0.60)] {
            let (when, event) = feed
                .prepare_server_event(0, receive, snapshot("up", 100, receive, ask, 50.0))
                .unwrap();
            assert_eq!(when, 100);
            apply(&mut core, true, event);
        }
        assert_eq!(core.raw_older_books_dropped, 0);
        assert_eq!(
            feed.stream_sequence[0], 3,
            "same timestamp is not an identity"
        );
        assert_eq!(
            core.submit_order(&bid("cross", 0.61), 100).status,
            OrderStatus::Rejected
        );
    }

    #[test]
    fn strict_sibling_books_use_raw_canonical_guard_and_stream_anchors_stay_isolated() {
        let mut feed = fixture_feed(true, 2);
        let mut core = fixture_core(true);
        let (_, first) = feed
            .prepare_server_event(0, 1000, snapshot("up", 200, 1000, 0.62, 50.0))
            .unwrap();
        apply(&mut core, true, first);
        // Metadata can advance this source's logical time, but cannot make an
        // older sibling snapshot new enough to overwrite the shared book.
        let (_, metadata) = feed
            .prepare_server_event(1, 400, MarketEvent::Instrument(instrument()))
            .unwrap();
        apply(&mut core, true, metadata);
        let (when, sibling) = feed
            .prepare_server_event(1, 1100, snapshot("down", 150, 1100, 0.95, 5.0))
            .unwrap();
        assert_eq!(when, 400);
        assert!(apply(&mut core, true, sibling).is_empty());
        assert_eq!(core.raw_older_books_dropped, 1);
        assert_eq!(
            core.submit_order(&bid("still-rests", 0.61), 400).status,
            OrderStatus::Accepted
        );
        feed.prepare_server_event(1, 1050, snapshot("up", 900, 1050, 0.62, 50.0));
        let (when, event) = feed
            .prepare_server_event(0, 1100, print("up", 1100))
            .unwrap();
        let SimEvent::ServerTrade(_, timing) = event else {
            unreachable!()
        };
        assert_eq!((when, timing.reconstructed_source_ns), (300, Some(300)));
        assert_eq!(timing.stream_index, 0);
    }

    #[test]
    fn strict_fresh_server_book_does_not_advance_local_visibility_clock() {
        let mut feed = fixture_feed(true, 1);
        let mut core = fixture_core(true);
        core.configure_book_stale_gate(15);
        let (_, event) = feed
            .prepare_server_event(0, 1000, snapshot("up", 100, 1000, 0.62, 50.0))
            .unwrap();
        apply(&mut core, true, event);
        core.submit_order(&bid("no-local", 0.59), 101);
        assert_eq!(core.book_stale_exchange_hits, 0);
        assert_eq!(
            core.book_stale_local_hits, 1,
            "future recorded receive is not already visible"
        );
    }

    #[test]
    fn legacy_clock_profile_keeps_rewritten_book_and_trade_anchor_behavior() {
        let mut feed = fixture_feed(false, 1);
        let mut core = fixture_core(true);
        for event in [snapshot("up", 100, 1000, 0.62, 50.0), print("up", 1020)] {
            let (_, prepared) = feed.prepare_server_event(0, 0, event).unwrap();
            apply(&mut core, false, prepared);
        }
        let (_, old) = feed
            .prepare_server_event(0, 1030, snapshot("up", 90, 1030, 0.59, 5.0))
            .unwrap();
        let SimEvent::ServerBook(ref payload, time) = old else {
            unreachable!()
        };
        assert_eq!(
            (payload.exchange_timestamp_ns, time.raw_source_ns),
            (120, 90)
        );
        apply(&mut core, false, old);
        assert_eq!(
            core.submit_order(&bid("legacy-cross", 0.61), 120).status,
            OrderStatus::Rejected
        );
        assert_eq!(core.raw_older_books_dropped, 0);
    }

    #[test]
    fn strict_source_anchors_are_bounded_and_eviction_has_explicit_fallback() {
        let mut anchors = SourceAnchors::new();
        for i in 0..=FEED_TOKEN_CAP {
            anchors.observe_book(&format!("token-{i}"), i as u64, i as u64);
        }
        assert_eq!(anchors.books.len(), FEED_TOKEN_CAP);
        assert_eq!(anchors.order.len(), FEED_TOKEN_CAP);
        assert!(!anchors.books.contains_key("token-0"));
        let (_, anchored) =
            reconstruct_trade_srv(anchors.books.get("token-0").copied(), None, 500, 500);
        assert!(
            !anchored,
            "retired token replay falls back explicitly; no fabricated old anchor"
        );
    }

    #[test]
    fn strict_server_feed_does_not_silently_skip_missing_sources() {
        let sources = vec![("polymarket".into(), "missing-token".into())];
        let start = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        let end = DateTime::<Utc>::from_timestamp(1, 0).unwrap();
        let result = ServerFeed::new_with_clock_options(
            Path::new("/nonexistent-strict-server-replay"),
            &sources,
            start,
            end,
            ReplayOptions::default(),
            true,
        );
        assert!(
            result.is_err(),
            "a strict source error cannot become a successful empty replay"
        );
        assert!(
            ServerFeed::new_with_clock_options(
                Path::new("/nonexistent-strict-server-replay"),
                &sources,
                start,
                end,
                ReplayOptions::default(),
                false,
            )
            .is_ok(),
            "legacy optional-source behavior stays compatible"
        );
    }

    #[test]
    fn strict_server_bootstrap_validation_also_covers_empty_source_lists() {
        let result = ServerFeed::new_with_clock_options(
            Path::new("/nonexistent"),
            &[],
            DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
            DateTime::<Utc>::from_timestamp(1, 0).unwrap(),
            ReplayOptions {
                time_policy: ReplayTimePolicy::ArrivalTimeStrict,
                bootstrap_binary_open: true,
                ..ReplayOptions::default()
            },
            true,
        );
        assert!(result.is_err());
    }

    /// Focused offline lane benchmark. Construct each input before timing;
    /// measure prepare_server_event entry through matching-core book return.
    /// Includes existing core allocations. No strategy, network, or live E2E
    /// claim; one synchronous owner, zero transport queue and zero overflow.
    #[test]
    #[ignore = "focused benchmark; run in release with --ignored --nocapture"]
    fn raw_server_clock_feed_core_benchmark() {
        const N: usize = 100_000;
        for strict in [false, true] {
            let mut feed = fixture_feed(strict, 1);
            let mut core = fixture_core(true);
            core.configure_maker_order_audit(false);
            let mut samples = Vec::with_capacity(N);
            for n in 0..(N + 1000) {
                let source = (n as u64 + 1) * 10;
                let event = snapshot("up", source, source + 1000, 0.62, 50.0);
                let before = std::time::Instant::now();
                let (_, prepared) = feed.prepare_server_event(0, source + 1000, event).unwrap();
                std::hint::black_box(apply(&mut core, strict, prepared));
                let elapsed = before.elapsed().as_nanos() as u64;
                if n >= 1000 {
                    samples.push(elapsed);
                }
            }
            samples.sort_unstable();
            println!("raw_server_clock_feed_core_benchmark strict={} events={} median_ns={} p99_ns={} p999_ns={} max_ns={} transport_queue_depth=0 transport_overflow=0 source_anchor_count={} source_anchor_capacity={}",
                strict, N, samples[N / 2], samples[N * 99 / 100], samples[N * 999 / 1000], samples[N - 1],
                feed.source_anchors[0].books.len(), FEED_TOKEN_CAP);
        }
    }

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

    #[test]
    fn server_lane_clamps_regression_without_using_local_time() {
        let (first, first_regression) = monotonic_server_time(0, 1_000);
        assert_eq!((first, first_regression), (1_000, 0));

        // A later locally-received frame may carry an older venue timestamp.
        // Server processing preserves native order at its prior logical time;
        // no local timestamp participates in this decision.
        let (second, second_regression) = monotonic_server_time(first, 975);
        assert_eq!((second, second_regression), (1_000, 25));

        let (third, third_regression) = monotonic_server_time(second, 1_025);
        assert_eq!((third, third_regression), (1_025, 0));
    }
}
