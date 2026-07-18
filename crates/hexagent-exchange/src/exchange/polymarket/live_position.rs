//! Live Polymarket position & balance management based on trade status.
//!
//! Tracks trades by their lifecycle (Matched → Mined → Confirmed/Failed) and
//! computes positions and balances with different confidence levels:
//! - `total_position()`: all non-FAILED trades (for quoter inventory)
//! - `confirmed_position()`: only CONFIRMED trades (for sell inventory checks)
//! - `available_balance()`: conservative cash estimate for buy order sizing

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use log::info;

use crate::types::Side;

// ════════════════════════════════════════════════════════════════
// User-feed health (narrow cross-thread handle)
// ════════════════════════════════════════════════════════════════

/// Health of the Polymarket user (fills) WebSocket feed, shared between the
/// feed task (writer) and the strategy (reader) as an `Arc<UserFeedHealth>`.
///
/// This is a *narrow* handle on purpose: the strategy must NOT read the full
/// `LivePositionManager` (its position/balance source of truth is its own
/// internal ledger), but it DOES need to know when the fill feed is
/// untrustworthy so it can pause quoting. Two independent conditions:
///
/// - `recovering`: the user WS is disconnected / reconnecting / replaying the
///   post-reconnect REST gap-fetch. The local ledger may be missing in-flight
///   fills → pause quoting until it clears (set false after gap replay).
/// - `inventory_uncertain`: the reconnect gap-replay hit its page cap with
///   trades still pending — we may have *permanently* missed fills. The
///   current event's inventory is unknowable; stop quoting/trading it and let
///   it ride to settlement. Cleared on the next event settlement.
#[derive(Debug)]
pub struct UserFeedHealth {
    recovering: AtomicBool,
    inventory_uncertain: AtomicBool,
}

impl UserFeedHealth {
    /// Starts `recovering=true`: until the feed's first connect + gap replay
    /// completes, the ledger isn't trustworthy and the strategy should wait.
    pub fn new() -> Self {
        Self {
            recovering: AtomicBool::new(true),
            inventory_uncertain: AtomicBool::new(false),
        }
    }
    pub fn is_recovering(&self) -> bool { self.recovering.load(Ordering::Relaxed) }
    pub fn set_recovering(&self, v: bool) { self.recovering.store(v, Ordering::Relaxed); }
    pub fn inventory_uncertain(&self) -> bool { self.inventory_uncertain.load(Ordering::Relaxed) }
    pub fn set_inventory_uncertain(&self, v: bool) { self.inventory_uncertain.store(v, Ordering::Relaxed); }
}

impl Default for UserFeedHealth {
    fn default() -> Self { Self::new() }
}

// ════════════════════════════════════════════════════════════════
// Taker-matched inventory (HTTP-response accelerator)
// ════════════════════════════════════════════════════════════════

/// A taker fill learned from the synchronous `POST /order` response
/// (`status:"matched"` with a single `tradeIDs` entry) BEFORE the
/// authoritative WS `user_feed` push lands. The HTTP response is both faster
/// and more in-order than the WS push (observed: HTTP always first, WS
/// occasionally delayed multi-second + out-of-order), so buffering it lets a
/// fast SELL-off stop over-committing against shares it has already sold but
/// not yet seen confirmed.
#[derive(Debug, Clone)]
struct TakerMatched {
    #[allow(dead_code)]
    order_id: String,
    asset_id: String, // clob token id (= OrderRequest.symbol)
    side: Side,
    size: f64, // full order quantity (single-trade match ⇒ size == filled)
}

#[derive(Default)]
struct TakerMatchedInner {
    /// trade_id → fill, for matches not yet seen on the WS feed.
    matched: HashMap<String, TakerMatched>,
    /// trade_ids already delivered by the WS feed. An HTTP add for a
    /// trade_id in here is dropped (WS already booked / will book it).
    /// Cleared together with `matched` per event.
    ws_seen: HashSet<String>,
}

/// Cross-thread temporary inventory buffer shared between the trade executor
/// (HTTP-reply thread = writer via [`Self::try_add`]) and the strategy
/// (reader), mirroring the [`UserFeedHealth`] narrow-handle pattern. The WS
/// `user_feed` push remains the authoritative ledger source; this buffer is a
/// short-lived accelerator that each entry vacates the moment its WS push
/// arrives ([`Self::on_ws_trade`]).
///
/// `enabled=false` makes every mutator a no-op and every reader return `0.0`,
/// so the live path is byte-identical to legacy. Backtest/paper never reach
/// the HTTP matched branch, so the buffer is always empty there regardless.
pub struct TakerMatchedInventory {
    enabled: AtomicBool,
    inner: Mutex<TakerMatchedInner>,
}

impl TakerMatchedInventory {
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            inner: Mutex::new(TakerMatchedInner::default()),
        }
    }

    pub fn set_enabled(&self, v: bool) { self.enabled.store(v, Ordering::Relaxed); }
    pub fn is_enabled(&self) -> bool { self.enabled.load(Ordering::Relaxed) }

    /// Record a taker fill from the `POST /order` matched response. Dropped
    /// if disabled, if the WS feed already delivered this `trade_id`
    /// (`ws_seen`), or if already buffered. Returns true if newly inserted.
    pub fn try_add(
        &self,
        trade_id: &str,
        order_id: &str,
        asset_id: &str,
        side: Side,
        size: f64,
    ) -> bool {
        if !self.is_enabled() || trade_id.is_empty() || size <= 0.0 {
            return false;
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.ws_seen.contains(trade_id) || inner.matched.contains_key(trade_id) {
            return false;
        }
        inner.matched.insert(trade_id.to_string(), TakerMatched {
            order_id: order_id.to_string(),
            asset_id: asset_id.to_string(),
            side,
            size,
        });
        true
    }

    /// A WS trade push arrived for `trade_id`: mark it seen (so a late HTTP
    /// add is dropped) and remove any buffered entry (WS is authoritative).
    /// Called BEFORE the corresponding `OrderUpdate` is delivered to the
    /// strategy, so the strategy never double-counts (buffer entry gone
    /// before the WS fill books into `PositionManager`).
    pub fn on_ws_trade(&self, trade_id: &str) {
        if trade_id.is_empty() { return; }
        let mut inner = self.inner.lock().unwrap();
        inner.matched.remove(trade_id);
        inner.ws_seen.insert(trade_id.to_string());
    }

    /// Drop all buffered state. Called on event rollover so an entry whose
    /// WS push was permanently lost can't linger past its event (where it
    /// would keep over-subtracting that token's sell inventory).
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.matched.clear();
        inner.ws_seen.clear();
    }

    /// Σ size of buffered SELL fills for `asset_id`. Subtract from
    /// `PositionManager::available_inventory` (which already nets WS-booked
    /// pending SELLs) to cover the HTTP→WS gap.
    pub fn sell_qty(&self, asset_id: &str) -> f64 {
        if !self.is_enabled() { return 0.0; }
        let inner = self.inner.lock().unwrap();
        inner.matched.values()
            .filter(|t| t.side == Side::Sell && t.asset_id == asset_id)
            .map(|t| t.size)
            .sum()
    }

    /// Net buffered position delta for `asset_id` (+BUY, −SELL). Add to the
    /// quoter's `get_quantity`-based position so inventory skew reflects
    /// taker fills before their WS push lands. A matched-but-unconfirmed BUY
    /// moves the quoter position (mirrors `get_quantity`) but does NOT enter
    /// `sell_qty`, so it never inflates sellable inventory.
    pub fn net_delta(&self, asset_id: &str) -> f64 {
        if !self.is_enabled() { return 0.0; }
        let inner = self.inner.lock().unwrap();
        inner.matched.values()
            .filter(|t| t.asset_id == asset_id)
            .map(|t| match t.side { Side::Buy => t.size, Side::Sell => -t.size })
            .sum()
    }

    #[cfg(test)]
    fn buffered_len(&self) -> usize { self.inner.lock().unwrap().matched.len() }
}

impl Default for TakerMatchedInventory {
    fn default() -> Self { Self::new() }
}

// ════════════════════════════════════════════════════════════════
// Trade Status
// ════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeStatus {
    Matched,
    Mined,
    Confirmed,
    Retrying,
    Failed,
}

impl TradeStatus {
    /// Parse from Polymarket status string (case-insensitive).
    pub fn from_str(s: &str) -> Option<Self> {
        let upper = s.to_uppercase();
        let normalized = upper.strip_prefix("TRADE_STATUS_").unwrap_or(&upper);
        match normalized {
            "MATCHED" => Some(Self::Matched),
            "MINED" => Some(Self::Mined),
            "CONFIRMED" => Some(Self::Confirmed),
            "RETRYING" => Some(Self::Retrying),
            "FAILED" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Whether this is a terminal state (no further updates expected).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Confirmed | Self::Failed)
    }

    /// Lifecycle stage rank for monotonic dedup: a status only advances the
    /// ledger when its rank is strictly greater than the current one. This
    /// makes repeated gap-replay / WS pushes idempotent (same rank → skip)
    /// and rejects out-of-order earlier states (lower rank → skip), so a
    /// stale `Matched` can never reverse a `Mined`/`Confirmed`.
    ///   Matched(1) → Mined(2) → Confirmed/Failed(3, terminal)
    /// `Retrying` is a transient (pre-resolution) state — rank 0, always
    /// skipped by the explicit `Retrying` guard, never written to the ledger.
    pub fn rank(&self) -> u8 {
        match self {
            Self::Retrying => 0,
            Self::Matched => 1,
            Self::Mined => 2,
            Self::Confirmed | Self::Failed => 3,
        }
    }
}

// ════════════════════════════════════════════════════════════════
// LiveTrade
// ════════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct LiveTrade {
    pub trade_id: String,
    pub asset_id: String,   // token ID
    pub side: Side,         // Buy or Sell
    pub size: f64,          // fill quantity
    pub price: f64,
    pub status: TradeStatus,
    pub is_maker: bool,
}

// ════════════════════════════════════════════════════════════════
// LivePositionManager  (WS fill log + gap-replay clock)
// ════════════════════════════════════════════════════════════════

/// ⚠ NOT an inventory / balance source. Live position & balance for the
/// strategy come from `account::PositionManager` (`ctx.pm`), fed by the same
/// WS `OrderUpdate` stream. This type's former position/balance API was dead
/// (its only reader was an always-`None` `strategy.live_position`, since
/// `set_live_position` was never wired) and was removed 2026-06-20 to stop it
/// being mistaken for the inventory source. See memory
/// `taker-matched-inventory-accelerator`.
///
/// What remains is two live functions the user feed depends on:
/// - `update_trade`: emits the `[LivePosition] Trade …` fill-lifecycle log
///   (Matched → Mined → Confirmed/Failed), the human-readable audit trail.
/// - `touch_match_time` / `last_match_time_secs`: high-water-mark of seen
///   `match_time`, used as the REST gap-replay `after=` lower bound.
pub struct LivePositionManager {
    /// Fill ledger, keyed by trade_id (taker) or `trade_id:order_id` (maker).
    /// Retained only to dedup status transitions and drive the lifecycle log.
    trades: HashMap<String, LiveTrade>,
    /// Largest `match_time` (unix seconds) seen so far. Used as the `after=`
    /// lower bound when replaying missed trades over REST after reconnect.
    last_match_time_secs: u64,
}

impl LivePositionManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self {
            trades: HashMap::new(),
            last_match_time_secs: 0,
        }
    }

    /// Largest `match_time` (unix seconds) seen so far. Used as the `after=`
    /// lower bound on the REST `/trades` gap-fetch call.
    pub fn last_match_time_secs(&self) -> u64 { self.last_match_time_secs }

    /// Bump the last-seen match_time if `ts > current`.
    pub fn touch_match_time(&mut self, ts_secs: u64) {
        if ts_secs > self.last_match_time_secs {
            self.last_match_time_secs = ts_secs;
        }
    }

    // ════════════════════════════════════════════════════════════
    // Trade ledger updates
    // ════════════════════════════════════════════════════════════

    /// Update or insert a trade. Returns true if the trade was actually updated.
    ///
    /// Rules:
    /// - CONFIRMED and FAILED are terminal — no further updates once reached.
    /// - RETRYING does not update the local status (preserves current state).
    pub fn update_trade(
        &mut self,
        trade_id: &str,
        status: TradeStatus,
        asset_id: &str,
        side: Side,
        size: f64,
        price: f64,
        is_maker: bool,
        // Optional revert / status reason (parsed from the upstream WS
        // payload). Logged when present so FAILED transitions surface
        // the actual chain-revert cause (e.g. `INSUFFICIENT_BALANCE`,
        // `INVALID_NONCE`) instead of being silent.
        reason: Option<&str>,
    ) -> bool {
        // Transient retry state: never written to the ledger (we wait for the
        // resolving Mined/Confirmed/Failed). Covers first-sighting too.
        if status == TradeStatus::Retrying {
            return false;
        }
        if let Some(existing) = self.trades.get(trade_id) {
            // Terminal state — do not update.
            if existing.status.is_terminal() {
                return false;
            }
            // Monotonic: only advance to a strictly-later stage. Same or
            // earlier rank → skip (dedups repeated pushes, blocks reversal).
            if status.rank() <= existing.status.rank() {
                return false;
            }
        }

        let is_new = !self.trades.contains_key(trade_id);
        self.trades.insert(trade_id.to_string(), LiveTrade {
            trade_id: trade_id.to_string(),
            asset_id: asset_id.to_string(),
            side,
            size,
            price,
            status,
            is_maker,
        });

        let reason_part = match reason {
            Some(s) if !s.is_empty() => format!(" reason=\"{}\"", s),
            _ => String::new(),
        };
        if is_new {
            info!("[LivePosition] Trade {} {} {} {:.2}@{:.4} status={:?} maker={}{}",
                trade_id, side, asset_id, size, price, status, is_maker, reason_part);
        } else {
            info!("[LivePosition] Trade {} status → {:?}{}", trade_id, status, reason_part);
        }

        true
    }

}

#[cfg(test)]
mod user_feed_health_tests {
    use super::UserFeedHealth;

    #[test]
    fn starts_recovering_so_strategy_waits_for_first_replay() {
        // Load-bearing: until the feed's first connect + gap replay completes,
        // the ledger isn't trustworthy, so the strategy must pause.
        let h = UserFeedHealth::new();
        assert!(h.is_recovering());
        assert!(!h.inventory_uncertain());
    }

    #[test]
    fn recovering_clears_after_replay_and_resets_on_disconnect() {
        let h = UserFeedHealth::new();
        h.set_recovering(false);
        assert!(!h.is_recovering());
        h.set_recovering(true); // disconnect
        assert!(h.is_recovering());
    }

    #[test]
    fn inventory_uncertain_is_independent_of_recovering() {
        let h = UserFeedHealth::new();
        h.set_recovering(false);
        h.set_inventory_uncertain(true); // gap-replay truncated
        assert!(h.inventory_uncertain());
        assert!(!h.is_recovering());
        h.set_inventory_uncertain(false); // cleared at settlement
        assert!(!h.inventory_uncertain());
    }
}

#[cfg(test)]
mod update_trade_dedup_tests {
    use super::*;
    use crate::types::Side;
    fn upd(m: &mut LivePositionManager, id: &str, s: TradeStatus) -> bool {
        m.update_trade(id, s, "TOK", Side::Sell, 10.0, 0.99, false, None)
    }

    #[test]
    fn advances_dedups_and_blocks_reversal() {
        let mut m = LivePositionManager::new();
        assert!(upd(&mut m, "t1", TradeStatus::Matched));    // first sighting
        assert!(!upd(&mut m, "t1", TradeStatus::Matched));   // same → skip (dedup)
        assert!(upd(&mut m, "t1", TradeStatus::Mined));      // advance
        assert!(!upd(&mut m, "t1", TradeStatus::Matched));   // earlier → skip (no reversal)
        assert!(upd(&mut m, "t1", TradeStatus::Confirmed));  // advance to terminal
        assert!(!upd(&mut m, "t1", TradeStatus::Failed));    // terminal → immutable
    }

    #[test]
    fn retrying_always_skipped() {
        let mut m = LivePositionManager::new();
        assert!(!upd(&mut m, "t1", TradeStatus::Retrying));  // transient, even first sighting
        assert!(upd(&mut m, "t1", TradeStatus::Matched));
        assert!(!upd(&mut m, "t1", TradeStatus::Retrying));  // still skipped
    }
}

#[cfg(test)]
mod taker_matched_tests {
    use super::*;
    use crate::types::Side;

    #[test]
    fn add_then_read_sell_and_net() {
        let tm = TakerMatchedInventory::new();
        assert!(tm.try_add("t1", "0xoid", "DOWN", Side::Sell, 10.0));
        // SELL: reduces sellable AND moves quoter position negative.
        assert_eq!(tm.sell_qty("DOWN"), 10.0);
        assert_eq!(tm.net_delta("DOWN"), -10.0);
        // Other token untouched.
        assert_eq!(tm.sell_qty("UP"), 0.0);
    }

    #[test]
    fn buy_moves_position_but_not_sellable() {
        // User-confirmed asymmetry: a matched-but-unconfirmed BUY affects the
        // quoter position (net_delta) but NOT sell availability (sell_qty).
        let tm = TakerMatchedInventory::new();
        tm.try_add("t1", "0xoid", "UP", Side::Buy, 10.0);
        assert_eq!(tm.sell_qty("UP"), 0.0);
        assert_eq!(tm.net_delta("UP"), 10.0);
    }

    #[test]
    fn ws_push_removes_entry_seamless_handoff() {
        let tm = TakerMatchedInventory::new();
        tm.try_add("t1", "0xoid", "DOWN", Side::Sell, 10.0);
        assert_eq!(tm.buffered_len(), 1);
        // WS push arrives → buffer vacates (PositionManager now owns it).
        tm.on_ws_trade("t1");
        assert_eq!(tm.buffered_len(), 0);
        assert_eq!(tm.sell_qty("DOWN"), 0.0);
        assert_eq!(tm.net_delta("DOWN"), 0.0);
    }

    #[test]
    fn ws_first_blocks_late_http_add() {
        // HTTP-after-WS race: WS already seen ⇒ the late HTTP add is dropped
        // (no double count).
        let tm = TakerMatchedInventory::new();
        tm.on_ws_trade("t1");
        assert!(!tm.try_add("t1", "0xoid", "DOWN", Side::Sell, 10.0));
        assert_eq!(tm.sell_qty("DOWN"), 0.0);
    }

    #[test]
    fn duplicate_http_add_ignored() {
        let tm = TakerMatchedInventory::new();
        assert!(tm.try_add("t1", "0xoid", "DOWN", Side::Sell, 10.0));
        assert!(!tm.try_add("t1", "0xoid", "DOWN", Side::Sell, 10.0));
        assert_eq!(tm.sell_qty("DOWN"), 10.0); // not 20
    }

    #[test]
    fn disabled_is_noop() {
        let tm = TakerMatchedInventory::new();
        tm.set_enabled(false);
        assert!(!tm.try_add("t1", "0xoid", "DOWN", Side::Sell, 10.0));
        assert_eq!(tm.sell_qty("DOWN"), 0.0);
        assert_eq!(tm.net_delta("DOWN"), 0.0);
    }

    #[test]
    fn clear_drops_buffer_and_seen() {
        let tm = TakerMatchedInventory::new();
        tm.try_add("t1", "0xoid", "DOWN", Side::Sell, 10.0);
        tm.on_ws_trade("t2"); // seen-only
        tm.clear();
        assert_eq!(tm.buffered_len(), 0);
        // ws_seen cleared too: a fresh add for t2 now succeeds.
        assert!(tm.try_add("t2", "0xoid", "UP", Side::Sell, 5.0));
    }

    #[test]
    fn zero_or_empty_rejected() {
        let tm = TakerMatchedInventory::new();
        assert!(!tm.try_add("", "0xoid", "DOWN", Side::Sell, 10.0));
        assert!(!tm.try_add("t1", "0xoid", "DOWN", Side::Sell, 0.0));
    }
}
