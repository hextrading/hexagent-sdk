//! Per-token L2 books + Polymarket cross-outcome synthetic view.
//!
//! Polymarket binary markets have two complementary tokens (up/down) whose
//! prices sum to ~1. `BUY up @ p ≡ SELL down @ (1−p)`, so liquidity is
//! *merged*: a taker buying `up` can match `up` asks OR `down` bids (mapped to
//! `1 − price`). This module maintains the raw per-token books and exposes the
//! merged "effective" ladders a taker actually sweeps (design doc §4).
//!
//! P3 reuses the same `BookSet` for the resting-queue model (visible depth at a
//! price level = merged direct + complement depth).

// Fast, deterministic hasher (FxHashMap) for the per-token book maps: keys are
// 77-char Polymarket token-id strings looked up many times per book event, so
// SipHash dominated replay CPU. Point-lookup only (never iterated for emission
// order), so the hasher swap is result-preserving.
use rustc_hash::FxHashMap as HashMap;
use std::cell::RefCell;
use std::rc::Rc;

use crate::types::{PriceLevel, Side};

/// Cached merged (buy, sell) ladders for one token. Each side is computed lazily
/// on first query and shared via `Rc` (cheap clone, no realloc); both are
/// invalidated (set back to `None`) when the token's — or its complement's —
/// book changes (`update`). Avoids rebuilding the merged ladder on every
/// `would_cross` / `submit_order` / `take` query within a single order's
/// processing (same book snapshot → ~3 queries → 1 build).
type LadderCache = (Option<Rc<Vec<PriceLevel>>>, Option<Rc<Vec<PriceLevel>>>);

/// Quantize a price to integer ticks (mirrors v1's `price_to_ticks`).
pub fn price_to_ticks(price: f64, tick: f64) -> i64 {
    if tick <= 0.0 {
        return (price * 1e12) as i64;
    }
    (price / tick).round() as i64
}

#[derive(Default, Clone)]
pub struct TokenBook {
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

#[derive(Default)]
pub struct BookSet {
    books: HashMap<String, TokenBook>,
    /// token ↔ complement token (binary markets).
    pairs: HashMap<String, String>,
    /// One-step lookahead snapshots for the "race" model (design: maker/taker
    /// race). Populated transiently by the simulator right before a placement /
    /// taker match from the feed's *next* book for the relevant token(s); read
    /// by `level_depth_next`. Empty ⇒ race off.
    next: HashMap<String, TokenBook>,
    /// Taker windowed race: ALL book snapshots over the in-flight horizon window
    /// for a token (canonical frame under folding). `available_volume_next` takes
    /// the MIN fillable volume across them — liquidity pulled at ANY instant in
    /// the window counts as a potential miss, not just the endpoint snapshot.
    next_window: HashMap<String, Vec<TokenBook>>,
    /// Outcome-folding flag (mirror of the exchange's `fold_outcomes`). Under
    /// folding the two outcomes are mirrored into ONE shared canonical book, so
    /// `pairs` is deliberately left empty and the cross-outcome merge below must
    /// stay inert — else `up.bid[p]` and the mirror `down.ask[1−p]` (the SAME
    /// liquidity, ~90% exact) are counted twice. Used only to `debug_assert` that
    /// invariant at the merge chokepoint (`comp_book`).
    folded: bool,
    /// Deep-queue model for a resting price BEYOND the recorded 5-level window
    /// (see `extrapolate_level_depth`): `0` = legacy least-squares linear
    /// extrapolation; `>0` = project from the OUTERMOST recorded level as
    /// `q_edge · decay^(ticks beyond window)` (`1.0` = flat at the outermost
    /// depth, `<1` = geometric thinning).
    deep_queue_decay: f64,
    /// Per-token cached merged ladders (see [`LadderCache`]). Interior-mutable so
    /// the `&self` ladder queries can memoise; invalidated by `update` /
    /// `set_pair` / `set_folded` / `set_deep_queue_decay`. Does NOT depend on the
    /// transient `next` / `next_window` lookahead books (those drive separate
    /// `*_next` queries), so the race priming never touches this cache.
    ladder_cache: RefCell<HashMap<String, LadderCache>>,
}

fn finite_levels(levels: &[PriceLevel]) -> impl Iterator<Item = &PriceLevel> {
    levels
        .iter()
        .filter(|l| l.quantity > 0.0 && l.price > 0.0 && l.price < 1.0)
}

/// Build the merged effective ladder a taker sweeps from a direct book and an
/// optional complement book. `is_buy`: `direct.asks` ∪ `comp.bids`→(1−p) sorted
/// ascending; else `direct.bids` ∪ `comp.asks`→(1−p) sorted descending.
fn merged_ladder(direct: Option<&TokenBook>, comp: Option<&TokenBook>, is_buy: bool) -> Vec<PriceLevel> {
    let mut v: Vec<PriceLevel> = Vec::new();
    if let Some(b) = direct {
        let levels = if is_buy { &b.asks } else { &b.bids };
        for l in finite_levels(levels) {
            v.push(PriceLevel { price: l.price, quantity: l.quantity });
        }
    }
    if let Some(cb) = comp {
        let levels = if is_buy { &cb.bids } else { &cb.asks };
        for l in finite_levels(levels) {
            v.push(PriceLevel { price: 1.0 - l.price, quantity: l.quantity });
        }
    }
    v.retain(|l| l.price > 0.0 && l.price < 1.0 && l.quantity > 0.0);
    if is_buy {
        v.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
    } else {
        v.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
    }
    v
}

/// Merged synthetic depth at our resting order's level (§5) from a direct book +
/// optional complement book. For a BUY (bid @ p): `direct.bids@p` +
/// `comp.asks@(1−p)`; for a SELL (ask @ p): `direct.asks@p` + `comp.bids@(1−p)`.
/// Queue depth at `price` on `side`, summing the direct book and (legacy only)
/// the complement's mirror level (`1−price`, bid↔ask). Under outcome-folding the
/// caller passes `comp = None` (see `comp_book`): the canonical book already holds
/// the mirrored down liquidity, so a non-None `comp` here would double-count the
/// SAME orders (up.bid[p] ≡ down.ask[1−p]).
fn merged_depth(direct: Option<&TokenBook>, comp: Option<&TokenBook>, side: Side, price: f64, tick: f64) -> f64 {
    let want = price_to_ticks(price, tick);
    let mut sum = 0.0;
    if let Some(b) = direct {
        let levels = match side {
            Side::Buy => &b.bids,
            Side::Sell => &b.asks,
        };
        for l in levels {
            if l.quantity > 0.0 && price_to_ticks(l.price, tick) == want {
                sum += l.quantity;
            }
        }
    }
    if let Some(cb) = comp {
        let mwant = price_to_ticks(1.0 - price, tick);
        let mlevels = match side {
            Side::Buy => &cb.asks,
            Side::Sell => &cb.bids,
        };
        for l in mlevels {
            if l.quantity > 0.0 && price_to_ticks(l.price, tick) == mwant {
                sum += l.quantity;
            }
        }
    }
    sum
}

/// Sum ladder volume reachable within `lim` (None = market: all of it).
fn within_volume(ladder: &[PriceLevel], is_buy: bool, lim: Option<f64>) -> f64 {
    ladder
        .iter()
        .filter(|l| match (lim, is_buy) {
            (None, _) => true,
            (Some(p), true) => l.price <= p + 1e-9,
            (Some(p), false) => l.price >= p - 1e-9,
        })
        .map(|l| l.quantity)
        .sum()
}

impl BookSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_pair(&mut self, a: &str, b: &str) {
        // Pairing enables the cross-outcome complement-merge (`comp_book`). It is
        // CORRECT only when the two books hold SEPARATE liquidity (legacy
        // non-folded path). Under folding the down book is mirrored into the
        // canonical book, so pairing would double-count the shared liquidity.
        debug_assert!(!self.folded, "set_pair must not be called under outcome-folding (would double-count mirror liquidity)");
        self.pairs.insert(a.to_string(), b.to_string());
        self.pairs.insert(b.to_string(), a.to_string());
        // Pairing changes the cross-outcome merge → any cached ladder is stale.
        self.ladder_cache.get_mut().clear();
    }

    /// Mirror of the exchange's `fold_outcomes`; guards the merge invariant.
    pub fn set_folded(&mut self, on: bool) {
        self.folded = on;
        self.ladder_cache.get_mut().clear();
    }

    /// Deep-queue model selector (see the `deep_queue_decay` field). 0 = legacy
    /// linear extrapolation; >0 = outermost-level flat/geometric-decay.
    pub fn set_deep_queue_decay(&mut self, d: f64) {
        self.deep_queue_decay = d.max(0.0);
        self.ladder_cache.get_mut().clear();
    }

    /// Drop the cached merged ladders for `token` and its complement — the
    /// complement's ladder merges this token's book, so a book change to either
    /// invalidates both. Called whenever `token`'s book is replaced.
    fn invalidate_ladder(&mut self, token: &str) {
        let comp = self.pairs.get(token).cloned();
        let cache = self.ladder_cache.get_mut();
        cache.remove(token);
        if let Some(c) = comp {
            cache.remove(&c);
        }
    }

    pub fn complement(&self, token: &str) -> Option<&String> {
        self.pairs.get(token)
    }

    pub fn update(&mut self, token: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        self.books.insert(token.to_string(), TokenBook { bids, asks });
        self.invalidate_ladder(token);
    }

    /// Permanently drop all state for a settled token — book, pairing, cached
    /// ladder, and any stale lookahead snapshot. Called by the exchange's
    /// event-retire hook to bound memory over long runs; the token's event has
    /// settled and it is never referenced again.
    pub fn retire_token(&mut self, token: &str) {
        self.books.remove(token);
        self.pairs.remove(token);
        self.next.remove(token);
        self.next_window.remove(token);
        self.ladder_cache.get_mut().remove(token);
    }

    /// Stash the *next* book snapshot for `token` (one-step race lookahead).
    pub fn set_next(&mut self, token: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        self.next.insert(token.to_string(), TokenBook { bids, asks });
    }
    /// Append a window snapshot for `token` (taker windowed race). Each call adds
    /// one book seen in the in-flight horizon; `available_volume_next` mins over
    /// all of them. Frames are stored in the canonical frame (caller mirrors).
    pub fn push_next_window(&mut self, token: &str, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) {
        self.next_window
            .entry(token.to_string())
            .or_default()
            .push(TokenBook { bids, asks });
    }
    /// Drop all stashed lookahead books (called before each priming).
    pub fn clear_next(&mut self) {
        self.next.clear();
        self.next_window.clear();
    }

    /// Raw (single-token) best bid/ask, ignoring cross-outcome liquidity.
    pub fn token_best_bid(&self, token: &str) -> Option<f64> {
        self.books.get(token).and_then(|b| {
            finite_levels(&b.bids).map(|l| l.price).fold(None, |m, p| Some(m.map_or(p, |x: f64| x.max(p))))
        })
    }
    pub fn token_best_ask(&self, token: &str) -> Option<f64> {
        self.books.get(token).and_then(|b| {
            finite_levels(&b.asks).map(|l| l.price).fold(None, |m, p| Some(m.map_or(p, |x: f64| x.min(p))))
        })
    }

    /// Effective ladder a taker BUYING `token` sweeps, cheapest first:
    /// `token.asks` ∪ `complement.bids` mapped to `1 − price`. Memoised per token
    /// (invalidated on `update`); the `Rc` clone is cheap and shared.
    pub fn buy_ladder(&self, token: &str) -> Rc<Vec<PriceLevel>> {
        if let Some(l) = self.ladder_cache.borrow().get(token).and_then(|e| e.0.clone()) {
            return l;
        }
        let l = Rc::new(merged_ladder(self.books.get(token), self.comp_book(&self.books, token), true));
        self.ladder_cache.borrow_mut().entry(token.to_string()).or_default().0 = Some(l.clone());
        l
    }

    /// Effective ladder a taker SELLING `token` sweeps, highest first:
    /// `token.bids` ∪ `complement.asks` mapped to `1 − price`. Memoised per token.
    pub fn sell_ladder(&self, token: &str) -> Rc<Vec<PriceLevel>> {
        if let Some(l) = self.ladder_cache.borrow().get(token).and_then(|e| e.1.clone()) {
            return l;
        }
        let l = Rc::new(merged_ladder(self.books.get(token), self.comp_book(&self.books, token), false));
        self.ladder_cache.borrow_mut().entry(token.to_string()).or_default().1 = Some(l.clone());
        l
    }

    /// Complement's book from `src`, falling back to the *current* book when the
    /// complement is absent from `src` (lets a one-sided `next` snapshot still
    /// merge cross-outcome depth from the live book).
    fn comp_book<'a>(&'a self, src: &'a HashMap<String, TokenBook>, token: &str) -> Option<&'a TokenBook> {
        // INVARIANT: under outcome-folding `pairs` is empty (see `on_instrument`),
        // so this returns None and the complement-merge in `level_depth` /
        // `buy_ladder` / `sell_ladder` stays inert — the canonical book already
        // carries the mirrored down liquidity, so merging it again would
        // double-count the same orders (up.bid[p] ≡ down.ask[1−p]).
        let comp = self.pairs.get(token)?;
        let res = src.get(comp).or_else(|| self.books.get(comp));
        debug_assert!(!self.folded || res.is_none(), "comp_book must be inert under folding (would double-count mirror liquidity)");
        res
    }

    /// Total effective volume a taker can fill within `lim` (None = market), on
    /// the *current* book. Used as the "now" leg of the taker race.
    pub fn available_volume(&self, token: &str, is_buy: bool, lim: Option<f64>) -> f64 {
        let ladder = if is_buy { self.buy_ladder(token) } else { self.sell_ladder(token) };
        within_volume(&ladder, is_buy, lim)
    }

    /// Same as `available_volume` but on the lookahead snapshot(s) — the "race"
    /// leg. When a window of snapshots was primed (`push_next_window`, taker
    /// windowed race) the MIN fillable volume over ALL of them is returned —
    /// liquidity pulled at ANY instant in the in-flight horizon counts as a
    /// potential miss, not just the endpoint. Falls back to the single `next`
    /// snapshot when no window was primed. `None` ⇒ no lookahead (race inactive).
    pub fn available_volume_next(&self, token: &str, is_buy: bool, lim: Option<f64>) -> Option<f64> {
        // Windowed taker race (folding only): each frame is a single canonical
        // book carrying all liquidity (caller mirrored siblings), so no
        // cross-outcome merge — take the MIN fillable volume over the window.
        if let Some(frames) = self.next_window.get(token) {
            if !frames.is_empty() {
                let mut min_vol = f64::INFINITY;
                for f in frames {
                    let ladder = merged_ladder(Some(f), None, is_buy);
                    min_vol = min_vol.min(within_volume(&ladder, is_buy, lim));
                }
                return Some(min_vol);
            }
        }
        let direct = self.next.get(token)?;
        let ladder = merged_ladder(Some(direct), self.comp_book(&self.next, token), is_buy);
        Some(within_volume(&ladder, is_buy, lim))
    }

    /// Visible synthetic depth at our resting order's level — the queue our
    /// order joins (design doc §5). For a resting BUY (a bid @ `price`):
    /// `token` bids @ price + complement asks @ (1−price). For a resting SELL
    /// (an ask @ price): `token` asks @ price + complement bids @ (1−price).
    /// Excludes our own order (it's not in the recorded book).
    pub fn level_depth(&self, token: &str, side: Side, price: f64, tick: f64) -> f64 {
        merged_depth(self.books.get(token), self.comp_book(&self.books, token), side, price, tick)
    }

    /// Merged queue length at our level on the stashed *next* snapshot (one-step
    /// race lookahead). `None` when there's no lookahead book for `token`.
    pub fn level_depth_next(&self, token: &str, side: Side, price: f64, tick: f64) -> Option<f64> {
        let direct = self.next.get(token)?;
        Some(merged_depth(Some(direct), self.comp_book(&self.next, token), side, price, tick))
    }

    /// Effective best ask for buying `token` (min over the buy ladder).
    pub fn eff_best_ask(&self, token: &str) -> Option<f64> {
        self.buy_ladder(token).first().map(|l| l.price)
    }
    /// Effective best bid for selling `token` (max over the sell ladder).
    pub fn eff_best_bid(&self, token: &str) -> Option<f64> {
        self.sell_ladder(token).first().map(|l| l.price)
    }
    /// Effective (merged) mid for the canonical frame: `(eff_best_bid +
    /// eff_best_ask)/2`, or the single available touch, or `0.0` when neither
    /// side has a book. Used as the adverse-selection signal in `resync_queues`
    /// (mid move against a resting order between snapshots). `0.0` ⇒ unknown.
    pub fn eff_mid(&self, token: &str) -> f64 {
        match (self.eff_best_bid(token), self.eff_best_ask(token)) {
            (Some(b), Some(a)) => 0.5 * (b + a),
            (Some(b), None) => b,
            (None, Some(a)) => a,
            (None, None) => 0.0,
        }
    }

    /// Total (merged) quantity at the best bid / best ask level. 0 if no such
    /// side. Used as a default queue length when a quote lands beyond the
    /// recorded book depth (5-level truncation → level_depth = 0).
    pub fn best_bid_qty(&self, token: &str, tick: f64) -> f64 {
        match self.eff_best_bid(token) {
            Some(p) => self.level_depth(token, Side::Buy, p, tick),
            None => 0.0,
        }
    }
    pub fn best_ask_qty(&self, token: &str, tick: f64) -> f64 {
        match self.eff_best_ask(token) {
            Some(p) => self.level_depth(token, Side::Sell, p, tick),
            None => 0.0,
        }
    }

    /// Extrapolated queue depth for an order whose price sits BEYOND the deepest
    /// recorded level on its side (the recorded book is 5-level truncated). The
    /// resting side for a SELL is the ask ladder, for a BUY the bid ladder; both
    /// are merged (cross-outcome) and grouped per tick. Returns:
    ///   * `None` if the price is at/within the recorded window (a gap inside the
    ///     book / inside the spread → caller keeps the best-level default rule),
    ///     or if there are < 2 recorded levels (can't fit a trend).
    ///   * `Some(qty)` otherwise: a least-squares linear fit of (tick-distance
    ///     from touch → level qty) over the recorded levels, evaluated at our
    ///     distance and clamped to the recorded [min, max] qty band (so the
    ///     projection stays within observed depths and never goes ≤ 0).
    pub fn extrapolate_level_depth(&self, token: &str, side: Side, price: f64, tick: f64) -> Option<f64> {
        // Merged resting ladder on our side, grouped per tick (price→qty).
        let ladder = match side {
            Side::Sell => self.buy_ladder(token),  // asks, ascending
            Side::Buy => self.sell_ladder(token),  // bids, descending
        };
        if ladder.is_empty() {
            return None;
        }
        let mut levels: Vec<(i64, f64)> = Vec::new();
        for l in ladder.iter() {
            let t = price_to_ticks(l.price, tick);
            match levels.iter_mut().find(|(lt, _)| *lt == t) {
                Some((_, q)) => *q += l.quantity,
                None => levels.push((t, l.quantity)),
            }
        }
        // Touch tick = best (ladder is already best-first).
        let touch = price_to_ticks(ladder[0].price, tick);
        let want = price_to_ticks(price, tick);
        // Signed tick distance from touch, deeper = larger positive.
        let dist = |t: i64| -> i64 {
            match side {
                Side::Sell => t - touch, // ask deeper = higher price
                Side::Buy => touch - t,  // bid deeper = lower price
            }
        };
        let our_d = dist(want) as f64;
        let edge_d = levels.iter().map(|(t, _)| dist(*t)).max().unwrap_or(0) as f64;
        // Only extrapolate when truly beyond the recorded window.
        if our_d <= edge_d {
            return None;
        }
        // Opt-in flat/geometric-decay model: project from the OUTERMOST recorded
        // level. decay=1 → flat (outermost depth verbatim); 0<decay<1 → geometric
        // thinning per tick beyond the window. Needs only the edge level.
        if self.deep_queue_decay > 0.0 {
            let q_edge = levels.iter().find(|(t, _)| dist(*t) as f64 == edge_d).map(|(_, q)| *q)?;
            return Some(q_edge * self.deep_queue_decay.powf(our_d - edge_d));
        }
        if levels.len() < 2 {
            return None;
        }
        // Least-squares line qty = a + b·dist over recorded levels.
        let n = levels.len() as f64;
        let xs: Vec<f64> = levels.iter().map(|(t, _)| dist(*t) as f64).collect();
        let ys: Vec<f64> = levels.iter().map(|(_, q)| *q).collect();
        let mx = xs.iter().sum::<f64>() / n;
        let my = ys.iter().sum::<f64>() / n;
        let sxx: f64 = xs.iter().map(|x| (x - mx) * (x - mx)).sum();
        let sxy: f64 = xs.iter().zip(&ys).map(|(x, y)| (x - mx) * (y - my)).sum();
        let b = if sxx > 0.0 { sxy / sxx } else { 0.0 };
        let a = my - b * mx;
        let est = a + b * our_d;
        let qmin = ys.iter().cloned().fold(f64::MAX, f64::min);
        let qmax = ys.iter().cloned().fold(0.0_f64, f64::max);
        Some(est.clamp(qmin, qmax))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(price: f64, quantity: f64) -> PriceLevel {
        PriceLevel { price, quantity }
    }

    #[test]
    fn single_token_best_prices() {
        let mut bs = BookSet::new();
        bs.update("up", vec![lvl(0.60, 100.0), lvl(0.59, 50.0)], vec![lvl(0.62, 80.0), lvl(0.63, 40.0)]);
        assert_eq!(bs.token_best_bid("up"), Some(0.60));
        assert_eq!(bs.token_best_ask("up"), Some(0.62));
    }

    #[test]
    fn cross_outcome_improves_effective_ask() {
        // up ask = 0.62; down bid = 0.40 → maps to up ask 0.60 (cheaper).
        let mut bs = BookSet::new();
        bs.set_pair("up", "down");
        bs.update("up", vec![lvl(0.58, 100.0)], vec![lvl(0.62, 80.0)]);
        bs.update("down", vec![lvl(0.40, 70.0)], vec![lvl(0.43, 50.0)]);
        // Buy ladder for up: [0.60(from down bid 0.40), 0.62(up ask)].
        let ladder = bs.buy_ladder("up");
        assert_eq!(ladder.len(), 2);
        assert!((ladder[0].price - 0.60).abs() < 1e-9);
        assert!((ladder[0].quantity - 70.0).abs() < 1e-9);
        assert!((ladder[1].price - 0.62).abs() < 1e-9);
        assert!((bs.eff_best_ask("up").unwrap() - 0.60).abs() < 1e-9);
    }

    #[test]
    fn cross_outcome_improves_effective_bid() {
        // up bid = 0.58; down ask = 0.43 → maps to up bid 0.57.
        // best effective bid for selling up = max(0.58, 0.57) = 0.58.
        let mut bs = BookSet::new();
        bs.set_pair("up", "down");
        bs.update("up", vec![lvl(0.58, 100.0)], vec![lvl(0.62, 80.0)]);
        bs.update("down", vec![lvl(0.40, 70.0)], vec![lvl(0.41, 50.0)]);
        // down ask 0.41 → up bid 0.59 (better than direct 0.58).
        let ladder = bs.sell_ladder("up");
        assert!((ladder[0].price - 0.59).abs() < 1e-9);
        assert!((ladder[0].quantity - 50.0).abs() < 1e-9);
        assert!((bs.eff_best_bid("up").unwrap() - 0.59).abs() < 1e-9);
    }

    #[test]
    fn no_complement_falls_back_to_direct() {
        let mut bs = BookSet::new();
        bs.update("up", vec![lvl(0.58, 100.0)], vec![lvl(0.62, 80.0)]);
        let ladder = bs.buy_ladder("up");
        assert_eq!(ladder.len(), 1);
        assert_eq!(bs.eff_best_ask("up"), Some(0.62));
    }
}
