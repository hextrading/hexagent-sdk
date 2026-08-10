//! Live Polymarket position & balance management based on trade status.
//!
//! Tracks trades by their lifecycle (Matched → Mined → Confirmed/Failed) and
//! computes positions and balances with different confidence levels:
//! - `total_position()`: all non-FAILED trades (for quoter inventory)
//! - `confirmed_position()`: only CONFIRMED trades (for sell inventory checks)
//! - `available_balance()`: conservative cash estimate for buy order sizing

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use log::info;

use crate::types::Side;
use hexagent_account::account::shared_account::RestoredTrade;

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
/// - `gap_replay_degraded`: the periodic REST safety net has failed repeatedly.
///   The WebSocket may still be connected, but until the pinned replay window
///   catches up we cannot prove that its local ledger is complete.
#[derive(Debug)]
pub struct UserFeedHealth {
    recovering: AtomicBool,
    inventory_uncertain: AtomicBool,
    gap_replay_degraded: AtomicBool,
}

impl UserFeedHealth {
    /// Starts `recovering=true`: until the feed's first connect + gap replay
    /// completes, the ledger isn't trustworthy and the strategy should wait.
    pub fn new() -> Self {
        Self {
            recovering: AtomicBool::new(true),
            inventory_uncertain: AtomicBool::new(false),
            gap_replay_degraded: AtomicBool::new(false),
        }
    }
    pub fn is_recovering(&self) -> bool { self.recovering.load(Ordering::Relaxed) }
    pub fn set_recovering(&self, v: bool) { self.recovering.store(v, Ordering::Relaxed); }
    pub fn inventory_uncertain(&self) -> bool { self.inventory_uncertain.load(Ordering::Relaxed) }
    pub fn set_inventory_uncertain(&self, v: bool) { self.inventory_uncertain.store(v, Ordering::Relaxed); }
    pub fn gap_replay_degraded(&self) -> bool {
        self.gap_replay_degraded.load(Ordering::Relaxed)
    }
    pub fn set_gap_replay_degraded(&self, v: bool) {
        self.gap_replay_degraded.store(v, Ordering::Relaxed);
    }
}

impl Default for UserFeedHealth {
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
            "MATCHED" | "MATCHED_NOT_BROADCASTED" => Some(Self::Matched),
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

    pub fn from_restored(rows: impl IntoIterator<Item = RestoredTrade>) -> Self {
        let mut manager = Self::new();
        for row in rows {
            manager.touch_match_time(row.match_time_secs);
            let Some(status) = TradeStatus::from_str(&row.ownership.status) else { continue; };
            manager.update_trade(
                &row.ownership.trade_key,
                status,
                &row.ownership.token_id,
                row.ownership.side,
                row.ownership.quantity,
                row.ownership.price,
                row.is_maker,
                None,
            );
        }
        manager
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
        if trade_id.trim().is_empty() || asset_id.trim().is_empty()
            || !size.is_finite() || size <= 0.0
            || !price.is_finite() || price <= 0.0 || price > 1.0 + 1e-8
        {
            return false;
        }
        if let Some(existing) = self.trades.get(trade_id) {
            let size_tolerance = 1e-8_f64.max(existing.size.abs() * 1e-8);
            let price_tolerance = 1e-10_f64.max(existing.price.abs() * 1e-8);
            if existing.asset_id != asset_id || existing.side != side
                || existing.is_maker != is_maker
                || (existing.size - size).abs() > size_tolerance
                || (existing.price - price).abs() > price_tolerance
            {
                return false;
            }
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

    pub fn prune_terminal_history(&mut self, tokens: &HashSet<String>) -> usize {
        let before = self.trades.len();
        self.trades.retain(|_, trade| {
            !tokens.contains(&trade.asset_id) || !trade.status.is_terminal()
        });
        before.saturating_sub(self.trades.len())
    }

    #[cfg(test)]
    fn trade_count(&self) -> usize { self.trades.len() }

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
        assert!(!h.gap_replay_degraded());
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

    #[test]
    fn gap_replay_degraded_is_independent_and_recoverable() {
        let h = UserFeedHealth::new();
        h.set_recovering(false);
        h.set_gap_replay_degraded(true);
        assert!(h.gap_replay_degraded());
        assert!(!h.is_recovering());
        assert!(!h.inventory_uncertain());
        h.set_gap_replay_degraded(false);
        assert!(!h.gap_replay_degraded());
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

    #[test]
    fn rejects_invalid_values_and_trade_identity_mutation() {
        let mut m = LivePositionManager::new();
        assert!(!m.update_trade(
            "bad", TradeStatus::Matched, "TOK", Side::Buy,
            f64::NAN, 0.4, true, None,
        ));
        assert!(m.update_trade(
            "strict", TradeStatus::Matched, "TOK", Side::Buy,
            5.0, 0.4, true, None,
        ));
        assert!(!m.update_trade(
            "strict", TradeStatus::Mined, "OTHER", Side::Buy,
            5.0, 0.4, true, None,
        ));
        assert!(!m.update_trade(
            "strict", TradeStatus::Mined, "TOK", Side::Sell,
            5.0, 0.4, true, None,
        ));
        assert!(!m.update_trade(
            "strict", TradeStatus::Mined, "TOK", Side::Buy,
            5.1, 0.4, true, None,
        ));
        assert!(!m.update_trade(
            "strict", TradeStatus::Mined, "TOK", Side::Buy,
            5.0, 0.41, true, None,
        ));
        assert!(!m.update_trade(
            "strict", TradeStatus::Mined, "TOK", Side::Buy,
            5.0, 0.4, false, None,
        ));
        assert!(m.update_trade(
            "strict", TradeStatus::Mined, "TOK", Side::Buy,
            5.0 + 1e-9, 0.4 + 1e-10, true, None,
        ));
    }

    #[test]
    fn prune_removes_only_terminal_rows_in_retired_token_scope() {
        let mut m = LivePositionManager::new();
        assert!(m.update_trade(
            "terminal", TradeStatus::Confirmed, "TOK", Side::Buy,
            1.0, 0.4, true, None,
        ));
        assert!(m.update_trade(
            "pending", TradeStatus::Matched, "TOK", Side::Buy,
            1.0, 0.4, true, None,
        ));
        assert!(m.update_trade(
            "other", TradeStatus::Failed, "OTHER", Side::Buy,
            1.0, 0.4, true, None,
        ));
        assert_eq!(m.prune_terminal_history(&HashSet::from(["TOK".into()])), 1);
        assert_eq!(m.trade_count(), 2);
    }
}
