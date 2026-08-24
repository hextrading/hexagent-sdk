//! Live Polymarket position & balance management based on trade status.
//!
//! Tracks trades by their lifecycle (Matched → Mined → Confirmed/Failed) and
//! computes positions and balances with different confidence levels:
//! - `total_position()`: all non-FAILED trades (for quoter inventory)
//! - `confirmed_position()`: only CONFIRMED trades (for sell inventory checks)
//! - `available_balance()`: conservative cash estimate for buy order sizing

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::types::{now_ns, OrderUpdate, Side};
use hexagent_account::account::shared_account::RestoredTrade;

/// Startup replay can precede strategy construction (prediction/APV2 warmup
/// is deliberately synchronous). Keep that race bounded without reconnecting
/// a healthy private socket merely because no strategy consumer exists yet.
const STARTUP_RECOVERY_BUFFER_CAPACITY: usize = 4_096;

// ════════════════════════════════════════════════════════════════
// User-feed health (narrow cross-thread handle)
// ════════════════════════════════════════════════════════════════

/// Health of the Polymarket user (fills) WebSocket feed, shared between the
/// feed task (writer) and the strategy (reader) as an `Arc<UserFeedHealth>`.
///
/// This is a *narrow* handle on purpose: the strategy must NOT read the full
/// `LivePositionManager` (its position/balance source of truth is its own
/// internal ledger), but it DOES need to know when the fill feed is
/// untrustworthy so it can pause quoting. Three independent conditions:
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
///   catches up we cannot prove that the REST safety net is current. This is
///   diagnostic/degraded state only while the private WS remains connected;
///   it does not by itself make inventory unknown or require quote pauses.
#[derive(Debug)]
pub struct UserFeedHealth {
    recovering: AtomicBool,
    /// Wall-clock edge timestamp for the current recovery interval. Strategies
    /// use it to pause new orders immediately while retaining resting maker
    /// orders across short reconnects.
    recovering_since_ns: AtomicU64,
    inventory_uncertain: AtomicBool,
    gap_replay_degraded: AtomicBool,
    last_transport_activity_ns: AtomicU64,
    last_valid_business_event_ns: AtomicU64,
    strategy_consumer_ready_fast: AtomicBool,
    strategy_consumer_ready_notify: tokio::sync::Notify,
    recovery_delivery: Mutex<RecoveryDeliveryState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RecoveryUpdateKey {
    instance_id: String,
    client_order_id: String,
    exchange_order_id: Option<String>,
    trade_id: Option<String>,
    symbol: String,
    status: String,
    filled_quantity_bits: u64,
    remaining_quantity_bits: u64,
}

impl RecoveryUpdateKey {
    fn new(instance_id: &str, update: &OrderUpdate) -> Self {
        Self {
            instance_id: instance_id.to_string(),
            client_order_id: update.client_order_id.clone(),
            exchange_order_id: update.exchange_order_id.clone(),
            trade_id: update.trade_id.clone(),
            symbol: update.symbol.clone(),
            status: format!("{:?}", update.status),
            filled_quantity_bits: update.filled_quantity.to_bits(),
            remaining_quantity_bits: update.remaining_quantity.to_bits(),
        }
    }
}

#[derive(Debug, Default)]
struct RecoveryDeliveryState {
    generation: u64,
    enrolling: bool,
    pending: HashMap<RecoveryUpdateKey, usize>,
    startup_buffer: VecDeque<(u64, OrderUpdate)>,
}

/// Completion token held across one strategy `on_order_update` call. It only
/// acknowledges on a normal return; a panicking worker leaves the recovery
/// epoch pending and therefore keeps account quoting paused.
#[derive(Debug)]
pub struct RecoveryUpdateAck {
    health: Arc<UserFeedHealth>,
    instance_id: String,
    update: OrderUpdate,
}

impl Drop for RecoveryUpdateAck {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = self
                .health
                .acknowledge_recovery_update(&self.instance_id, &self.update);
        }
    }
}

impl UserFeedHealth {
    /// Starts `recovering=true`: until the feed's first connect + gap replay
    /// completes, the ledger isn't trustworthy and the strategy should wait.
    pub fn new() -> Self {
        Self {
            recovering: AtomicBool::new(true),
            recovering_since_ns: AtomicU64::new(now_ns()),
            inventory_uncertain: AtomicBool::new(false),
            gap_replay_degraded: AtomicBool::new(false),
            last_transport_activity_ns: AtomicU64::new(0),
            last_valid_business_event_ns: AtomicU64::new(0),
            strategy_consumer_ready_fast: AtomicBool::new(false),
            strategy_consumer_ready_notify: tokio::sync::Notify::new(),
            recovery_delivery: Mutex::new(RecoveryDeliveryState::default()),
        }
    }
    pub fn is_recovering(&self) -> bool {
        self.recovering.load(Ordering::Acquire)
    }
    pub fn set_recovering(&self, v: bool) {
        if v {
            if !self.recovering.swap(true, Ordering::AcqRel) {
                self.recovering_since_ns.store(now_ns(), Ordering::Release);
            } else if self.recovering_since_ns.load(Ordering::Acquire) == 0 {
                self.recovering_since_ns.store(now_ns(), Ordering::Release);
            }
        } else {
            self.recovering.store(false, Ordering::Release);
            self.recovering_since_ns.store(0, Ordering::Release);
        }
    }
    /// Elapsed wall-clock recovery time. Returns zero while healthy and also
    /// for the tiny publication race at the beginning of a recovery edge.
    pub fn recovering_for_ns(&self, current_ns: u64) -> u64 {
        if !self.is_recovering() {
            return 0;
        }
        let since = self.recovering_since_ns.load(Ordering::Acquire);
        current_ns.saturating_sub(since)
    }
    pub fn inventory_uncertain(&self) -> bool {
        self.inventory_uncertain.load(Ordering::Relaxed)
    }
    pub fn set_inventory_uncertain(&self, v: bool) {
        self.inventory_uncertain.store(v, Ordering::Relaxed);
    }
    pub fn gap_replay_degraded(&self) -> bool {
        self.gap_replay_degraded.load(Ordering::Relaxed)
    }
    pub fn set_gap_replay_degraded(&self, v: bool) {
        self.gap_replay_degraded.store(v, Ordering::Relaxed);
    }
    pub fn record_transport_activity(&self, timestamp_ns: u64) {
        self.last_transport_activity_ns
            .fetch_max(timestamp_ns, Ordering::Relaxed);
    }
    pub fn record_valid_business_event(&self, timestamp_ns: u64) {
        self.last_valid_business_event_ns
            .fetch_max(timestamp_ns, Ordering::Relaxed);
    }
    pub fn activity_timestamps_ns(&self) -> (u64, u64) {
        (
            self.last_transport_activity_ns.load(Ordering::Relaxed),
            self.last_valid_business_event_ns.load(Ordering::Relaxed),
        )
    }

    /// Start one reconnect-delivery epoch. The user feed registers every
    /// replay/open-order update before putting it on the engine channel; the
    /// owning strategy acknowledges it only after `on_order_update` returns.
    /// Quoting therefore cannot resume merely because recovery updates were
    /// enqueued while their PositionManager is still stale.
    pub fn begin_recovery_delivery(&self) -> u64 {
        self.set_recovering(true);
        let mut delivery = self.recovery_delivery.lock().unwrap();
        delivery.generation = delivery.generation.wrapping_add(1).max(1);
        delivery.enrolling = true;
        delivery.pending.clear();
        delivery.startup_buffer.clear();
        delivery.generation
    }

    pub fn register_recovery_update(
        &self,
        generation: u64,
        instance_id: &str,
        update: &OrderUpdate,
    ) -> Result<bool, String> {
        if instance_id.trim().is_empty() {
            return Err(format!(
                "recovery update coid={} has no owning instance",
                update.client_order_id,
            ));
        }
        let mut delivery = self.recovery_delivery.lock().unwrap();
        if delivery.generation != generation || !delivery.enrolling {
            return Err(format!(
                "recovery delivery generation {} is no longer accepting updates",
                generation,
            ));
        }
        // `mark_strategy_consumer_ready` publishes while holding this same
        // mutex, so an update is either wholly buffered before the edge or
        // wholly sent after it; the startup drain cannot miss the race.
        let buffer_update = !self.strategy_consumer_ready_fast.load(Ordering::Acquire);
        if buffer_update && delivery.startup_buffer.len() >= STARTUP_RECOVERY_BUFFER_CAPACITY {
            return Err(format!(
                "startup recovery buffer is full ({STARTUP_RECOVERY_BUFFER_CAPACITY} updates)",
            ));
        }
        *delivery
            .pending
            .entry(RecoveryUpdateKey::new(instance_id, update))
            .or_insert(0) += 1;
        if buffer_update {
            delivery
                .startup_buffer
                .push_back((generation, update.clone()));
        }
        Ok(buffer_update)
    }

    /// Publish the engine's order-update consumer after strategy construction.
    /// Buffered updates are drained by the user-feed recovery task, preserving
    /// its single enrollment/delivery ordering.
    pub fn mark_strategy_consumer_ready(&self) {
        let _delivery = self.recovery_delivery.lock().unwrap();
        self.strategy_consumer_ready_fast
            .store(true, Ordering::Release);
        self.strategy_consumer_ready_notify.notify_one();
    }

    pub fn strategy_consumer_ready(&self) -> bool {
        self.strategy_consumer_ready_fast.load(Ordering::Acquire)
    }

    pub async fn wait_for_strategy_consumer_ready(&self) {
        while !self.strategy_consumer_ready() {
            self.strategy_consumer_ready_notify.notified().await;
        }
    }

    pub fn take_startup_recovery_updates(
        &self,
        generation: u64,
    ) -> Result<Vec<OrderUpdate>, String> {
        let mut delivery = self.recovery_delivery.lock().unwrap();
        if delivery.generation != generation {
            return Err(format!(
                "recovery delivery generation {generation} was superseded",
            ));
        }
        if !self.strategy_consumer_ready_fast.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let mut updates = Vec::with_capacity(delivery.startup_buffer.len());
        while let Some((buffered_generation, update)) = delivery.startup_buffer.pop_front() {
            if buffered_generation == generation {
                updates.push(update);
            }
        }
        Ok(updates)
    }

    pub fn finish_recovery_delivery_enrollment(&self, generation: u64) -> bool {
        let mut delivery = self.recovery_delivery.lock().unwrap();
        if delivery.generation != generation {
            return false;
        }
        delivery.enrolling = false;
        true
    }

    /// Called by the owning strategy after it has fully processed an update.
    /// Broadcast siblings cannot consume the acknowledgement because the
    /// instance id is part of the key.
    pub fn acknowledge_recovery_update(&self, instance_id: &str, update: &OrderUpdate) -> bool {
        let mut delivery = self.recovery_delivery.lock().unwrap();
        let key = RecoveryUpdateKey::new(instance_id, update);
        let Some(count) = delivery.pending.get_mut(&key) else {
            return false;
        };
        if *count > 1 {
            *count -= 1;
        } else {
            delivery.pending.remove(&key);
        }
        true
    }

    pub fn recovery_update_ack(
        self: &Arc<Self>,
        instance_id: &str,
        update: &OrderUpdate,
    ) -> Option<RecoveryUpdateAck> {
        let delivery = self.recovery_delivery.lock().unwrap();
        if delivery.pending.is_empty() {
            return None;
        }
        let key = RecoveryUpdateKey::new(instance_id, update);
        delivery
            .pending
            .contains_key(&key)
            .then(|| RecoveryUpdateAck {
                health: Arc::clone(self),
                instance_id: instance_id.to_string(),
                update: update.clone(),
            })
    }

    /// Returns `None` if a newer reconnect superseded this generation;
    /// otherwise `(enrollment_finished, pending_update_count)`.
    pub fn recovery_delivery_progress(&self, generation: u64) -> Option<(bool, usize)> {
        let delivery = self.recovery_delivery.lock().unwrap();
        (delivery.generation == generation).then(|| {
            (
                !delivery.enrolling,
                delivery.pending.values().copied().sum(),
            )
        })
    }
}

impl Default for UserFeedHealth {
    fn default() -> Self {
        Self::new()
    }
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
    pub asset_id: String, // token ID
    pub side: Side,       // Buy or Sell
    pub size: f64,        // fill quantity
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
        let receipt_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        for row in rows {
            // Keep the upstream timestamp in the durable row, but never
            // restore a future REST replay lower bound after restart.
            manager.touch_match_time(row.match_time_secs.min(receipt_secs));
            let Some(status) = TradeStatus::from_str(&row.ownership.status) else {
                continue;
            };
            manager.update_trade_inner(
                &row.ownership.trade_key,
                status,
                &row.ownership.token_id,
                row.ownership.side,
                row.ownership.quantity,
                row.ownership.price,
                row.is_maker,
                None,
                false,
            );
        }
        manager
    }

    /// Largest `match_time` (unix seconds) seen so far. Used as the `after=`
    /// lower bound on the REST `/trades` gap-fetch call.
    pub fn last_match_time_secs(&self) -> u64 {
        self.last_match_time_secs
    }

    /// Bump the conservative REST replay watermark if `ts > current`.
    /// Callers must pass receipt-capped time, never an unchecked upstream
    /// business timestamp.
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
        self.update_trade_inner(
            trade_id, status, asset_id, side, size, price, is_maker, reason, true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn update_trade_inner(
        &mut self,
        trade_id: &str,
        status: TradeStatus,
        asset_id: &str,
        side: Side,
        size: f64,
        price: f64,
        is_maker: bool,
        reason: Option<&str>,
        log_transition: bool,
    ) -> bool {
        // Transient retry state: never written to the ledger (we wait for the
        // resolving Mined/Confirmed/Failed). Covers first-sighting too.
        if status == TradeStatus::Retrying {
            return false;
        }
        if trade_id.trim().is_empty()
            || asset_id.trim().is_empty()
            || !size.is_finite()
            || size <= 0.0
            || !price.is_finite()
            || price <= 0.0
            || price > 1.0 + 1e-8
        {
            return false;
        }
        if let Some(existing) = self.trades.get(trade_id) {
            let size_tolerance = 1e-8_f64.max(existing.size.abs() * 1e-8);
            let price_tolerance = 1e-10_f64.max(existing.price.abs() * 1e-8);
            if existing.asset_id != asset_id
                || existing.side != side
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
        self.trades.insert(
            trade_id.to_string(),
            LiveTrade {
                trade_id: trade_id.to_string(),
                asset_id: asset_id.to_string(),
                side,
                size,
                price,
                status,
                is_maker,
            },
        );

        if !log_transition {
            // Restored rows are summarized by the account startup log. Per-row
            // lifecycle output here used to dominate cold-start logs.
        } else if log::log_enabled!(log::Level::Debug) {
            let trade_id = trade_id.to_string();
            let asset_id = asset_id.to_string();
            let reason = reason.filter(|value| !value.is_empty()).map(str::to_string);
            let _ = hexagent_runtime::background_jobs::try_submit(move || {
                let reason_part = reason
                    .as_deref()
                    .map(|value| format!(" reason=\"{value}\""))
                    .unwrap_or_default();
                if is_new {
                    log::debug!(
                        "[LivePosition] Trade {} {} {} {:.2}@{:.4} status={:?} maker={}{}",
                        trade_id,
                        side,
                        asset_id,
                        size,
                        price,
                        status,
                        is_maker,
                        reason_part,
                    );
                } else {
                    log::debug!(
                        "[LivePosition] Trade {} status → {:?}{}",
                        trade_id,
                        status,
                        reason_part,
                    );
                }
            });
        }

        true
    }

    pub fn prune_terminal_history(&mut self, tokens: &HashSet<String>) -> usize {
        let before = self.trades.len();
        self.trades
            .retain(|_, trade| !tokens.contains(&trade.asset_id) || !trade.status.is_terminal());
        before.saturating_sub(self.trades.len())
    }

    #[cfg(test)]
    fn trade_count(&self) -> usize {
        self.trades.len()
    }
}

#[cfg(test)]
mod user_feed_health_tests {
    use super::UserFeedHealth;
    use crate::types::{now_ns, Exchange, OrderStatus, OrderUpdate, Side};

    fn recovery_update(coid: &str) -> OrderUpdate {
        OrderUpdate {
            client_order_id: coid.to_string(),
            exchange: Exchange::Polymarket,
            symbol: "TOKEN".to_string(),
            side: Side::Buy,
            exchange_order_id: Some("0x1".to_string()),
            status: OrderStatus::PartiallyFilled,
            liquidity: None,
            filled_quantity: 2.0,
            remaining_quantity: 3.0,
            avg_fill_price: 0.4,
            timestamp_ns: 1,
            exchange_event_timestamp_ns: None,
            trade_id: Some("trade-1".to_string()),
            order_audit: None,
            error: None,
        }
    }

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
    fn transport_and_business_health_clocks_are_independent() {
        let h = UserFeedHealth::new();
        h.record_transport_activity(10);
        assert_eq!(h.activity_timestamps_ns(), (10, 0));
        h.record_valid_business_event(8);
        assert_eq!(h.activity_timestamps_ns(), (10, 8));
        h.record_transport_activity(12);
        assert_eq!(h.activity_timestamps_ns(), (12, 8));
    }

    #[test]
    fn recovering_clears_after_replay_and_resets_on_disconnect() {
        let h = UserFeedHealth::new();
        h.set_recovering(false);
        assert!(!h.is_recovering());
        h.set_recovering(true); // disconnect
        assert!(h.is_recovering());
        assert!(h.recovering_for_ns(now_ns().saturating_add(1)) > 0);
        h.set_recovering(false);
        assert_eq!(h.recovering_for_ns(now_ns()), 0);
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

    #[test]
    fn recovery_delivery_waits_for_the_owning_worker() {
        let h = std::sync::Arc::new(UserFeedHealth::new());
        let generation = h.begin_recovery_delivery();
        let update = recovery_update("btc01-1");
        h.register_recovery_update(generation, "btc01", &update)
            .unwrap();
        assert!(h.finish_recovery_delivery_enrollment(generation));
        assert_eq!(h.recovery_delivery_progress(generation), Some((true, 1)));

        assert!(!h.acknowledge_recovery_update("btc02", &update));
        assert_eq!(h.recovery_delivery_progress(generation), Some((true, 1)));
        assert!(h.acknowledge_recovery_update("btc01", &update));
        assert_eq!(h.recovery_delivery_progress(generation), Some((true, 0)));
    }

    #[test]
    fn startup_recovery_is_buffered_until_strategy_consumer_is_ready() {
        let h = UserFeedHealth::new();
        let generation = h.begin_recovery_delivery();
        let update = recovery_update("btc01-buffered");
        assert!(h
            .register_recovery_update(generation, "btc01", &update)
            .unwrap());
        assert!(h
            .take_startup_recovery_updates(generation)
            .unwrap()
            .is_empty());
        h.mark_strategy_consumer_ready();
        assert!(h.strategy_consumer_ready());
        assert_eq!(
            h.take_startup_recovery_updates(generation).unwrap().len(),
            1,
        );
        assert!(h
            .take_startup_recovery_updates(generation)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn recovery_ack_guard_acknowledges_only_after_scope_exit() {
        let h = std::sync::Arc::new(UserFeedHealth::new());
        let generation = h.begin_recovery_delivery();
        let update = recovery_update("btc01-2");
        h.register_recovery_update(generation, "btc01", &update)
            .unwrap();
        assert!(h.finish_recovery_delivery_enrollment(generation));
        {
            let guard = h.recovery_update_ack("btc01", &update);
            assert!(guard.is_some());
            assert_eq!(h.recovery_delivery_progress(generation), Some((true, 1)));
        }
        assert_eq!(h.recovery_delivery_progress(generation), Some((true, 0)));
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
        assert!(upd(&mut m, "t1", TradeStatus::Matched)); // first sighting
        assert!(!upd(&mut m, "t1", TradeStatus::Matched)); // same → skip (dedup)
        assert!(upd(&mut m, "t1", TradeStatus::Mined)); // advance
        assert!(!upd(&mut m, "t1", TradeStatus::Matched)); // earlier → skip (no reversal)
        assert!(upd(&mut m, "t1", TradeStatus::Confirmed)); // advance to terminal
        assert!(!upd(&mut m, "t1", TradeStatus::Failed)); // terminal → immutable
    }

    #[test]
    fn retrying_always_skipped() {
        let mut m = LivePositionManager::new();
        assert!(!upd(&mut m, "t1", TradeStatus::Retrying)); // transient, even first sighting
        assert!(upd(&mut m, "t1", TradeStatus::Matched));
        assert!(!upd(&mut m, "t1", TradeStatus::Retrying)); // still skipped
    }

    #[test]
    fn rejects_invalid_values_and_trade_identity_mutation() {
        let mut m = LivePositionManager::new();
        assert!(!m.update_trade(
            "bad",
            TradeStatus::Matched,
            "TOK",
            Side::Buy,
            f64::NAN,
            0.4,
            true,
            None,
        ));
        assert!(m.update_trade(
            "strict",
            TradeStatus::Matched,
            "TOK",
            Side::Buy,
            5.0,
            0.4,
            true,
            None,
        ));
        assert!(!m.update_trade(
            "strict",
            TradeStatus::Mined,
            "OTHER",
            Side::Buy,
            5.0,
            0.4,
            true,
            None,
        ));
        assert!(!m.update_trade(
            "strict",
            TradeStatus::Mined,
            "TOK",
            Side::Sell,
            5.0,
            0.4,
            true,
            None,
        ));
        assert!(!m.update_trade(
            "strict",
            TradeStatus::Mined,
            "TOK",
            Side::Buy,
            5.1,
            0.4,
            true,
            None,
        ));
        assert!(!m.update_trade(
            "strict",
            TradeStatus::Mined,
            "TOK",
            Side::Buy,
            5.0,
            0.41,
            true,
            None,
        ));
        assert!(!m.update_trade(
            "strict",
            TradeStatus::Mined,
            "TOK",
            Side::Buy,
            5.0,
            0.4,
            false,
            None,
        ));
        assert!(m.update_trade(
            "strict",
            TradeStatus::Mined,
            "TOK",
            Side::Buy,
            5.0 + 1e-9,
            0.4 + 1e-10,
            true,
            None,
        ));
    }

    #[test]
    fn prune_removes_only_terminal_rows_in_retired_token_scope() {
        let mut m = LivePositionManager::new();
        assert!(m.update_trade(
            "terminal",
            TradeStatus::Confirmed,
            "TOK",
            Side::Buy,
            1.0,
            0.4,
            true,
            None,
        ));
        assert!(m.update_trade(
            "pending",
            TradeStatus::Matched,
            "TOK",
            Side::Buy,
            1.0,
            0.4,
            true,
            None,
        ));
        assert!(m.update_trade(
            "other",
            TradeStatus::Failed,
            "OTHER",
            Side::Buy,
            1.0,
            0.4,
            true,
            None,
        ));
        assert_eq!(m.prune_terminal_history(&HashSet::from(["TOK".into()])), 1);
        assert_eq!(m.trade_count(), 2);
    }

    #[test]
    fn restored_future_business_time_cannot_restore_a_future_replay_watermark() {
        let receipt_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let restored = RestoredTrade {
            ownership: hexagent_account::account::shared_account::TradeOwnership {
                account_id: "account".to_string(),
                instance_id: "instance".to_string(),
                trade_key: "future-trade".to_string(),
                client_order_id: "coid".to_string(),
                order_id: "order".to_string(),
                token_id: "TOKEN".to_string(),
                side: Side::Buy,
                quantity: 1.0,
                price: 0.5,
                status: "MATCHED".to_string(),
            },
            booked: true,
            usdc_fee: 0.0,
            shares_fee: 0.0,
            virtual_fee_booked: true,
            is_maker: true,
            match_time_secs: receipt_secs.saturating_add(3_600),
            ledger_generation: 1,
        };
        let manager = LivePositionManager::from_restored([restored]);
        assert!(manager.last_match_time_secs() <= receipt_secs);
    }
}
