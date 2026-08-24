use anyhow::{anyhow, Result};
use ed25519_dalek::SigningKey;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::sdk::auth::{build_order_message, ed25519_sign};
use super::sdk::{
    HexClient, HexClientConfig, OrderType as SdkOrderType, PlaceOrderParams, Side as SdkSide,
    TimeInForce as SdkTimeInForce,
};

use crate::exchange::ExchangeTrade;
use crate::types::*;

use super::auth::resolve_auth;

const HEXMARKET_OPEN_ORDER_CAPACITY: usize = 8_192;
const HEXMARKET_ORDER_IDENTITY_BYTES: usize = 96;
const HEXMARKET_SYMBOL_BYTES: usize = 128;
const HEXMARKET_EXCHANGE_ORDER_ID_BYTES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FixedText<const N: usize> {
    len: u8,
    bytes: [u8; N],
}

impl<const N: usize> FixedText<N> {
    #[inline]
    fn try_from_str(value: &str, field: &str) -> Result<Self> {
        if value.len() > N || value.len() > usize::from(u8::MAX) {
            return Err(anyhow!(
                "Hexmarket {field} exceeds fixed inline capacity ({}/{N} bytes)",
                value.len()
            ));
        }
        let mut bytes = [0; N];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        Ok(Self {
            len: value.len() as u8,
            bytes,
        })
    }

    #[inline]
    fn as_str(&self) -> &str {
        // Values are copied from an already-valid UTF-8 `str` and never mutated.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[..usize::from(self.len)]) }
    }

    #[inline]
    fn matches(&self, value: &str) -> bool {
        usize::from(self.len) == value.len()
            && self.bytes[..usize::from(self.len)] == *value.as_bytes()
    }
}

/// Local record of an open order.
#[derive(Debug, Clone)]
struct TrackedOrder {
    exchange_order_id: Option<FixedText<HEXMARKET_EXCHANGE_ORDER_ID_BYTES>>,
    symbol: FixedText<HEXMARKET_SYMBOL_BYTES>,
    side: Side,
    order_slot: OrderSlot,
}

#[derive(Debug)]
enum OpenOrderCell {
    Empty,
    Occupied {
        coid: FixedText<HEXMARKET_ORDER_IDENTITY_BYTES>,
        order: TrackedOrder,
    },
}

/// Startup-allocated, single-owner identity table.
///
/// The physical connection task is the only reader/writer. Open addressing
/// keeps submit/cancel tracking free of locks, rehashes and heap-growing maps.
#[derive(Debug)]
struct OpenOrderTable {
    cells: Box<[OpenOrderCell]>,
    len: usize,
}

impl OpenOrderTable {
    fn new() -> Self {
        Self::with_capacity(HEXMARKET_OPEN_ORDER_CAPACITY)
    }

    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity > 0, "open-order table capacity must be non-zero");
        let cells = (0..capacity)
            .map(|_| OpenOrderCell::Empty)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { cells, len: 0 }
    }

    #[inline]
    fn hash(value: &str) -> usize {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for byte in value.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash as usize
    }

    #[inline]
    #[cfg(test)]
    fn len(&self) -> usize {
        self.len
    }

    fn contains(&self, coid: &str) -> bool {
        let mut index = Self::hash(coid) % self.cells.len();
        for _ in 0..self.cells.len() {
            match &self.cells[index] {
                OpenOrderCell::Empty => return false,
                OpenOrderCell::Occupied { coid: key, .. } if key.matches(coid) => return true,
                OpenOrderCell::Occupied { .. } => {}
            }
            index = (index + 1) % self.cells.len();
        }
        false
    }

    fn get_order(&self, coid: &str) -> Option<&TrackedOrder> {
        let mut index = Self::hash(coid) % self.cells.len();
        for _ in 0..self.cells.len() {
            match &self.cells[index] {
                OpenOrderCell::Empty => return None,
                OpenOrderCell::Occupied { coid: key, order } if key.matches(coid) => {
                    return Some(order);
                }
                OpenOrderCell::Occupied { .. } => {}
            }
            index = (index + 1) % self.cells.len();
        }
        None
    }

    fn prepare(&self, coid: &str, symbol: &str) -> Result<PreparedTrackedIdentity> {
        let coid = FixedText::try_from_str(coid, "client order id")?;
        let symbol = FixedText::try_from_str(symbol, "symbol")?;
        Ok(PreparedTrackedIdentity { coid, symbol })
    }

    fn ensure_capacity_for(&self, identities: &[PreparedTrackedIdentity]) -> Result<()> {
        let additions = identities
            .iter()
            .enumerate()
            .filter(|(index, identity)| {
                let duplicate = identities[..*index]
                    .iter()
                    .any(|prior| prior.coid == identity.coid);
                if duplicate {
                    return false;
                }
                !self.contains(identity.coid.as_str())
            })
            .count();
        let final_len = self.len.saturating_add(additions);
        if final_len > self.cells.len() {
            return Err(anyhow!(
                "Hexmarket fixed open-order table exhausted (need {final_len}, capacity {})",
                self.cells.len()
            ));
        }
        Ok(())
    }

    fn insert_prepared(
        &mut self,
        identity: PreparedTrackedIdentity,
        exchange_order_id: Option<&str>,
        side: Side,
        order_slot: OrderSlot,
    ) -> Result<()> {
        let exchange_order_id = exchange_order_id
            .map(|value| FixedText::try_from_str(value, "exchange order id"))
            .transpose()?;
        self.insert_tracked(
            identity.coid,
            TrackedOrder {
                exchange_order_id,
                symbol: identity.symbol,
                side,
                order_slot,
            },
        )
    }

    fn insert_tracked(
        &mut self,
        coid: FixedText<HEXMARKET_ORDER_IDENTITY_BYTES>,
        order: TrackedOrder,
    ) -> Result<()> {
        let mut index = Self::hash(coid.as_str()) % self.cells.len();
        for _ in 0..self.cells.len() {
            match &self.cells[index] {
                OpenOrderCell::Empty => {
                    self.cells[index] = OpenOrderCell::Occupied {
                        coid,
                        order,
                    };
                    self.len += 1;
                    return Ok(());
                }
                OpenOrderCell::Occupied { coid: key, .. } if *key == coid => {
                    self.cells[index] = OpenOrderCell::Occupied {
                        coid,
                        order,
                    };
                    return Ok(());
                }
                OpenOrderCell::Occupied { .. } => {}
            }
            index = (index + 1) % self.cells.len();
        }
        Err(anyhow!("Hexmarket fixed open-order table exhausted"))
    }

    fn remove(&mut self, coid: &str) -> Option<TrackedOrder> {
        let mut index = Self::hash(coid) % self.cells.len();
        for _ in 0..self.cells.len() {
            match &self.cells[index] {
                OpenOrderCell::Empty => return None,
                OpenOrderCell::Occupied { coid: key, .. } if key.matches(coid) => {
                    let old = std::mem::replace(&mut self.cells[index], OpenOrderCell::Empty);
                    if let OpenOrderCell::Occupied { order, .. } = old {
                        self.len -= 1;
                        // Back-shift this probe cluster so lookup can still
                        // terminate on Empty and never accumulates tombstones.
                        let mut next = (index + 1) % self.cells.len();
                        for _ in 0..self.cells.len().saturating_sub(1) {
                            if matches!(&self.cells[next], OpenOrderCell::Empty) {
                                break;
                            }
                            let displaced =
                                std::mem::replace(&mut self.cells[next], OpenOrderCell::Empty);
                            if let OpenOrderCell::Occupied { coid, order } = displaced {
                                self.len -= 1;
                                self.insert_tracked(coid, order)
                                    .expect("removed order leaves room for cluster reinsert");
                            }
                            next = (next + 1) % self.cells.len();
                        }
                        return Some(order);
                    }
                    unreachable!();
                }
                OpenOrderCell::Occupied { .. } => {}
            }
            index = (index + 1) % self.cells.len();
        }
        None
    }

    fn drain_with(&mut self, mut emit: impl FnMut(String, TrackedOrder)) {
        for cell in self.cells.iter_mut() {
            let old = std::mem::replace(cell, OpenOrderCell::Empty);
            if let OpenOrderCell::Occupied { coid, order } = old {
                emit(coid.as_str().to_owned(), order);
            }
        }
        self.len = 0;
    }
}

#[derive(Debug, Clone, Copy)]
struct PreparedTrackedIdentity {
    coid: FixedText<HEXMARKET_ORDER_IDENTITY_BYTES>,
    symbol: FixedText<HEXMARKET_SYMBOL_BYTES>,
}

#[inline]
fn emit_fixed_update(out: &mut OrderUpdateBatch, update: OrderUpdate) -> Result<()> {
    push_order_update(out, update).map_err(|overflow| {
        anyhow!(
            "Hexmarket fixed lifecycle output exhausted at coid {}",
            overflow.update.client_order_id
        )
    })
}

/// Immutable/authentication state shared by physical connection owners.
struct SharedState {
    nonce: AtomicU64,
    signing_key: Option<SigningKey>,
    api_url_prefix: String,
    /// Cached pubkey + credentials for cloning workers
    pubkey: Option<String>,
    credentials: Option<super::sdk::ApiCredentials>,
    /// Per-wallet rate limiter (shared across all workers of same instance)
    rate_limiter: crate::exchange::AtomicRateLimiter,
}

/// HexMarket live order executor.
///
/// Each clone is one physical-connection owner with its own HTTP client and
/// open-order table. The dispatcher must keep one market/account shard sticky
/// to the same clone; no request worker shares mutable order identity state.
pub struct HexmarketTrade {
    shared: Arc<SharedState>,
    client: HexClient,
    open_orders: OpenOrderTable,
}

impl HexmarketTrade {
    pub fn new(
        private_key: &str,
        mnemonic: &str,
        api_url_prefix: &str,
        rate_limit_per_second: u32,
    ) -> Self {
        use super::auth::api_url_prefix_or_default;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let api_url_prefix = api_url_prefix_or_default(api_url_prefix);
        let mut client = HexClient::new(HexClientConfig {
            api_url: api_url_prefix.to_string(),
        });

        let has_key = !private_key.is_empty() || !mnemonic.is_empty();
        let (signing_key, pubkey, credentials) = if has_key {
            match resolve_auth(private_key, mnemonic, api_url_prefix) {
                Ok(auth) => {
                    client.set_credentials(&auth.pubkey, auth.credentials.clone());
                    (
                        Some(auth.signing_key),
                        Some(auth.pubkey),
                        Some(auth.credentials),
                    )
                }
                Err(e) => {
                    log::error!("[HexmarketTrade] Failed to resolve auth: {}", e);
                    (None, None, None)
                }
            }
        } else {
            (None, None, None)
        };

        let shared = Arc::new(SharedState {
            nonce: AtomicU64::new(now_ms),
            signing_key,
            api_url_prefix: api_url_prefix.to_string(),
            pubkey,
            credentials,
            rate_limiter: crate::exchange::AtomicRateLimiter::new(rate_limit_per_second),
        });

        Self {
            shared,
            client,
            open_orders: OpenOrderTable::new(),
        }
    }

    /// Create a parallel worker clone with its own HTTP client but shared state.
    pub fn clone_worker(&self) -> Self {
        let mut client = HexClient::new(HexClientConfig {
            api_url: self.shared.api_url_prefix.clone(),
        });
        if let (Some(pubkey), Some(creds)) = (&self.shared.pubkey, &self.shared.credentials) {
            client.set_credentials(pubkey, creds.clone());
        }
        Self {
            shared: Arc::clone(&self.shared),
            client,
            open_orders: OpenOrderTable::new(),
        }
    }

    /// Check rate limit. Returns Err with rejection reason if limit exceeded.
    fn check_rate_limit(&self) -> Result<()> {
        if self.shared.rate_limiter.try_acquire() {
            Ok(())
        } else {
            Err(anyhow!(
                "Rate limit exceeded ({}/s per wallet)",
                self.shared.rate_limiter.max_per_second(),
            ))
        }
    }

    fn next_nonce(&self) -> u64 {
        self.shared.nonce.fetch_add(1, Ordering::Relaxed)
    }

    fn sign_order(
        &self,
        outcome_id: &str,
        side: &str,
        price: &str,
        quantity: u64,
        nonce: u64,
    ) -> Result<String> {
        let signing_key = self
            .shared
            .signing_key
            .as_ref()
            .ok_or_else(|| anyhow!("No signing key configured"))?;
        let message = build_order_message(outcome_id, side, price, quantity, nonce);
        Ok(ed25519_sign(signing_key, &message))
    }

    /// Format an SDK error for logging. The local client returns
    /// `anyhow::Error` with status + body already embedded, so we just
    /// defer to Display.
    fn format_sdk_error(e: anyhow::Error) -> String {
        format!("{}", e)
    }

    fn build_order_params(&self, order: &OrderRequest) -> Result<PlaceOrderParams> {
        let side = match order.side {
            Side::Buy => SdkSide::Buy,
            Side::Sell => SdkSide::Sell,
        };
        let order_type = match order.order_type {
            OrderType::Market => SdkOrderType::Market,
            // Hexmarket SDK has only Market / Limit. Polymarket-specific
            // FAK / FOK shouldn't reach this code path (different
            // exchange routing) but we map them to Limit defensively
            // so an accidental cross-exchange signal doesn't panic.
            OrderType::Limit | OrderType::LimitMaker | OrderType::Fak | OrderType::Fok => {
                SdkOrderType::Limit
            }
        };
        let price = Decimal::from_f64(order.price.unwrap_or(0.0)).unwrap_or(Decimal::ZERO);
        let nonce = self.next_nonce();
        let side_str = match order.side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };

        let signature = self.sign_order(
            &order.symbol,
            side_str,
            &price.to_string(),
            order.quantity as u64,
            nonce,
        )?;

        Ok(PlaceOrderParams {
            outcome_id: order.symbol.clone(),
            side,
            order_type,
            time_in_force: SdkTimeInForce::Gtc,
            price,
            quantity: order.quantity as u64,
            nonce,
            signature,
            client_order_id: Some(order.client_order_id.clone()),
            session_pubkey: None,
            amount: None,
        })
    }
}

impl ExchangeTrade for HexmarketTrade {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        // Reject before touching the network if this physical owner cannot
        // represent the lifecycle identity losslessly.
        let tracked_identity = self
            .open_orders
            .prepare(&order.client_order_id, &order.symbol)?;
        self.open_orders
            .ensure_capacity_for(std::slice::from_ref(&tracked_identity))?;
        if let Err(e) = self.check_rate_limit() {
            return Ok(OrderUpdate {
                order_slot: order.order_slot,
                client_order_id: order.client_order_id.clone(),
                exchange: Exchange::Hexmarket,
                symbol: order.symbol.clone(),
                side: order.side,
                exchange_order_id: None,
                status: OrderStatus::Rejected,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: order.quantity,
                avg_fill_price: 0.0,
                timestamp_ns: now_ns(),
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: Some(e.to_string()),
            });
        }

        let params = self.build_order_params(order)?;
        let coid = &order.client_order_id;

        match self.client.place_order(&params) {
            Ok(resp) => {
                self.open_orders.insert_prepared(
                    tracked_identity,
                    Some(resp.order_id.as_str()),
                    order.side,
                    order.order_slot,
                )?;
                Ok(OrderUpdate {
                    order_slot: order.order_slot,
                    client_order_id: coid.clone(),
                    exchange: Exchange::Hexmarket,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    exchange_order_id: Some(resp.order_id),
                    status: OrderStatus::Accepted,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: order.quantity,
                    avg_fill_price: 0.0,
                    timestamp_ns: now_ns(),
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: None,
                })
            }
            Err(e) => {
                let detail = Self::format_sdk_error(e);
                Ok(OrderUpdate {
                    order_slot: order.order_slot,
                    client_order_id: coid.clone(),
                    exchange: Exchange::Hexmarket,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    exchange_order_id: None,
                    status: OrderStatus::Rejected,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: order.quantity,
                    avg_fill_price: 0.0,
                    timestamp_ns: now_ns(),
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: Some(detail),
                })
            }
        }
    }

    fn cancel_order(&mut self, _exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        self.check_rate_limit()?;

        // Preserve identity until the remote cancellation is acknowledged.
        // Errors are converted by the completion owner into CancelUncertain.
        self.client.cancel_order_by_client_id(client_order_id)?;
        let tracked = self.open_orders.remove(client_order_id);

        Ok(OrderUpdate {
            order_slot: tracked
                .as_ref()
                .map(|t| t.order_slot)
                .unwrap_or_default(),
            client_order_id: client_order_id.to_string(),
            exchange: Exchange::Hexmarket,
            symbol: tracked
                .as_ref()
                .map(|t| t.symbol.as_str().to_owned())
                .unwrap_or_default(),
            side: tracked.as_ref().map(|t| t.side).unwrap_or(Side::Buy),
            exchange_order_id: tracked
                .and_then(|t| t.exchange_order_id)
                .map(|oid| oid.as_str().to_owned()),
            status: OrderStatus::Cancelled,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: 0.0,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            order_audit: None,
            error: None,
        })
    }

    fn cancel_all(&mut self, exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        let mut updates = Vec::new();
        self.cancel_all_with(exchange, symbol, &mut |update| {
            updates.push(update);
            true
        })?;
        Ok(updates)
    }

    fn cancel_all_with(
        &mut self,
        _exchange: Exchange,
        _symbol: &str,
        emit: &mut dyn FnMut(OrderUpdate) -> bool,
    ) -> Result<()> {
        self.check_rate_limit()?;
        self.client.cancel_all_orders(None, None)?;

        let now = now_ns();
        let mut delivery_open = true;
        self.open_orders.drain_with(|coid, t| {
            let update = OrderUpdate {
                order_slot: t.order_slot,
                client_order_id: coid,
                exchange: Exchange::Hexmarket,
                symbol: t.symbol.as_str().to_owned(),
                side: t.side,
                exchange_order_id: t
                    .exchange_order_id
                    .map(|oid| oid.as_str().to_owned()),
                status: OrderStatus::Cancelled,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now,
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: None,
            };
            if delivery_open {
                delivery_open = emit(update);
            }
        });

        Ok(())
    }

    fn batch_submit_orders(
        &mut self,
        market_id: &str,
        orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        let mut out = OrderUpdateBatch::new();
        self.batch_submit_orders_into(market_id, orders, &mut out)?;
        Ok(out.into_iter().collect())
    }

    fn batch_submit_orders_into(
        &mut self,
        market_id: &str,
        orders: &[OrderRequest],
        out: &mut OrderUpdateBatch,
    ) -> Result<()> {
        if orders.len() > ORDER_BATCH_CAPACITY {
            return Err(anyhow!("Hexmarket place batch exceeds fixed capacity"));
        }
        let mut tracked_identities =
            arrayvec::ArrayVec::<PreparedTrackedIdentity, ORDER_BATCH_CAPACITY>::new();
        for order in orders {
            tracked_identities.push(
                self.open_orders
                    .prepare(&order.client_order_id, &order.symbol)?,
            );
        }
        self.open_orders
            .ensure_capacity_for(&tracked_identities)?;
        if let Err(e) = self.check_rate_limit() {
            let detail = e.to_string();
            let now = now_ns();
            for o in orders {
                emit_fixed_update(out, OrderUpdate {
                    order_slot: o.order_slot,
                    client_order_id: o.client_order_id.clone(),
                    exchange: Exchange::Hexmarket,
                    symbol: o.symbol.clone(),
                    side: o.side,
                    exchange_order_id: None,
                    status: OrderStatus::Rejected,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: o.quantity,
                    avg_fill_price: 0.0,
                    timestamp_ns: now,
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: Some(detail.clone()),
                })?;
            }
            return Ok(());
        }
        let mut params_list = arrayvec::ArrayVec::<PlaceOrderParams, ORDER_BATCH_CAPACITY>::new();
        for order in orders {
            params_list
                .try_push(self.build_order_params(order)?)
                .map_err(|_| anyhow!("Hexmarket place batch exceeds fixed capacity"))?;
        }

        let resp = self.client.batch_place_orders(market_id, &params_list)?;
        let now = now_ns();
        for (i, order) in orders.iter().enumerate() {
            let result = resp.results.get(i);
            let rejection = result
                .and_then(|result| result.error.as_deref())
                .or_else(|| result.is_none().then_some("missing batch place result"));
            if let Some(detail) = rejection {
                emit_fixed_update(out, OrderUpdate {
                    order_slot: order.order_slot,
                    client_order_id: order.client_order_id.clone(),
                    exchange: Exchange::Hexmarket,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    exchange_order_id: None,
                    status: OrderStatus::Rejected,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: order.quantity,
                    avg_fill_price: 0.0,
                    timestamp_ns: now,
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: Some(detail.to_owned()),
                })?;
                continue;
            }
            let oid = result.and_then(|result| result.order_id.clone());
            self.open_orders.insert_prepared(
                tracked_identities[i],
                oid.as_deref(),
                order.side,
                order.order_slot,
            )?;
            emit_fixed_update(out, OrderUpdate {
                order_slot: order.order_slot,
                client_order_id: order.client_order_id.clone(),
                exchange: Exchange::Hexmarket,
                symbol: order.symbol.clone(),
                side: order.side,
                exchange_order_id: oid,
                status: OrderStatus::Accepted,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: order.quantity,
                avg_fill_price: 0.0,
                timestamp_ns: now,
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: None,
            })?;
        }
        Ok(())
    }

    fn batch_cancel_orders(
        &mut self,
        exchange: Exchange,
        market_id: &str,
        client_order_ids: &[String],
    ) -> Result<Vec<OrderUpdate>> {
        let mut out = OrderUpdateBatch::new();
        self.batch_cancel_orders_into(exchange, market_id, client_order_ids, &mut out)?;
        Ok(out.into_iter().collect())
    }

    fn batch_cancel_orders_into(
        &mut self,
        _exchange: Exchange,
        market_id: &str,
        client_order_ids: &[String],
        out: &mut OrderUpdateBatch,
    ) -> Result<()> {
        self.check_rate_limit()?;
        if client_order_ids.is_empty() {
            return Ok(());
        }
        if client_order_ids.len() > ORDER_BATCH_CAPACITY {
            return Err(anyhow!("Hexmarket cancel batch exceeds fixed capacity"));
        }
        let coid_refs: arrayvec::ArrayVec<&str, ORDER_BATCH_CAPACITY> =
            client_order_ids.iter().map(String::as_str).collect();
        let resp = self
            .client
            .batch_cancel_orders(market_id, &[], &coid_refs)?;

        let now = now_ns();
        for (index, coid) in client_order_ids.iter().enumerate() {
            let result = resp
                .results
                .iter()
                .find(|result| result.client_order_id.as_deref() == Some(coid.as_str()))
                .or_else(|| resp.results.get(index));
            let error = result
                .and_then(|result| result.error.as_deref())
                .or_else(|| result.is_none().then_some("missing batch cancel result"));
            let tracked = if error.is_none() {
                self.open_orders.remove(coid)
            } else {
                self.open_orders.get_order(coid).cloned()
            };
            emit_fixed_update(out, OrderUpdate {
                order_slot: tracked
                    .as_ref()
                    .map(|order| order.order_slot)
                    .unwrap_or_default(),
                client_order_id: coid.to_owned(),
                exchange: Exchange::Hexmarket,
                symbol: tracked
                    .as_ref()
                    .map(|o| o.symbol.as_str().to_owned())
                    .unwrap_or_default(),
                side: tracked.as_ref().map(|o| o.side).unwrap_or(Side::Buy),
                exchange_order_id: tracked
                    .and_then(|o| o.exchange_order_id)
                    .map(|oid| oid.as_str().to_owned()),
                status: if error.is_none() {
                    OrderStatus::Cancelled
                } else {
                    OrderStatus::CancelUncertain
                },
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now,
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: error.map(str::to_owned),
            })?;
        }
        Ok(())
    }

    fn batch_update_orders(
        &mut self,
        exchange: Exchange,
        market_id: &str,
        cancel_client_order_ids: &[String],
        place_orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        let mut out = OrderUpdateBatch::new();
        self.batch_update_orders_into(
            exchange,
            market_id,
            cancel_client_order_ids,
            place_orders,
            &mut out,
        )?;
        Ok(out.into_iter().collect())
    }

    fn batch_update_orders_into(
        &mut self,
        _exchange: Exchange,
        market_id: &str,
        cancel_client_order_ids: &[String],
        place_orders: &[OrderRequest],
        out: &mut OrderUpdateBatch,
    ) -> Result<()> {
        if cancel_client_order_ids.len() > ORDER_BATCH_CAPACITY
            || place_orders.len() > ORDER_BATCH_CAPACITY
        {
            return Err(anyhow!("Hexmarket update batch exceeds fixed capacity"));
        }
        let mut place_identities =
            arrayvec::ArrayVec::<PreparedTrackedIdentity, ORDER_BATCH_CAPACITY>::new();
        for order in place_orders {
            place_identities.push(
                self.open_orders
                    .prepare(&order.client_order_id, &order.symbol)?,
            );
        }
        self.open_orders
            .ensure_capacity_for(&place_identities)?;
        self.check_rate_limit()?;

        let mut params_list = arrayvec::ArrayVec::<PlaceOrderParams, ORDER_BATCH_CAPACITY>::new();
        for order in place_orders {
            params_list
                .try_push(self.build_order_params(order)?)
                .map_err(|_| anyhow!("Hexmarket update place batch exceeds fixed capacity"))?;
        }

        let cancel_refs: arrayvec::ArrayVec<&str, ORDER_BATCH_CAPACITY> =
            cancel_client_order_ids.iter().map(String::as_str).collect();
        let resp = self
            .client
            .batch_update_orders(market_id, &[], &params_list, Some(&cancel_refs))?;
        let now = now_ns();

        for (index, coid) in cancel_client_order_ids.iter().enumerate() {
            let result = resp
                .cancel_results
                .iter()
                .find(|result| result.client_order_id.as_deref() == Some(coid.as_str()))
                .or_else(|| resp.cancel_results.get(index));
            let error = result
                .and_then(|result| result.error.as_deref())
                .or_else(|| result.is_none().then_some("missing batch update cancel result"));
            let tracked = if error.is_none() {
                self.open_orders.remove(coid)
            } else {
                self.open_orders.get_order(coid).cloned()
            };
            emit_fixed_update(out, OrderUpdate {
                order_slot: tracked
                    .as_ref()
                    .map(|order| order.order_slot)
                    .unwrap_or_default(),
                client_order_id: coid.clone(),
                exchange: Exchange::Hexmarket,
                symbol: tracked
                    .as_ref()
                    .map(|order| order.symbol.as_str().to_owned())
                    .unwrap_or_default(),
                side: tracked.as_ref().map(|order| order.side).unwrap_or(Side::Buy),
                exchange_order_id: tracked
                    .and_then(|order| order.exchange_order_id)
                    .map(|oid| oid.as_str().to_owned()),
                status: if error.is_none() {
                    OrderStatus::Cancelled
                } else {
                    OrderStatus::CancelUncertain
                },
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: 0.0,
                avg_fill_price: 0.0,
                timestamp_ns: now,
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: error.map(str::to_owned),
            })?;
        }

        for (index, order) in place_orders.iter().enumerate() {
            let result = resp.place_results.get(index);
            let rejection = result
                .and_then(|result| result.error.as_deref())
                .or_else(|| result.is_none().then_some("missing batch update place result"));
            if let Some(detail) = rejection {
                emit_fixed_update(out, OrderUpdate {
                    order_slot: order.order_slot,
                    client_order_id: order.client_order_id.clone(),
                    exchange: Exchange::Hexmarket,
                    symbol: order.symbol.clone(),
                    side: order.side,
                    exchange_order_id: None,
                    status: OrderStatus::Rejected,
                    liquidity: None,
                    filled_quantity: 0.0,
                    remaining_quantity: order.quantity,
                    avg_fill_price: 0.0,
                    timestamp_ns: now,
                    exchange_event_timestamp_ns: None,
                    trade_id: None,
                    order_audit: None,
                    error: Some(detail.to_owned()),
                })?;
                continue;
            }
            let oid = result.and_then(|result| result.order_id.clone());
            self.open_orders.insert_prepared(
                place_identities[index],
                oid.as_deref(),
                order.side,
                order.order_slot,
            )?;
            emit_fixed_update(out, OrderUpdate {
                order_slot: order.order_slot,
                client_order_id: order.client_order_id.clone(),
                exchange: Exchange::Hexmarket,
                symbol: order.symbol.clone(),
                side: order.side,
                exchange_order_id: oid,
                status: OrderStatus::Accepted,
                liquidity: None,
                filled_quantity: 0.0,
                remaining_quantity: order.quantity,
                avg_fill_price: 0.0,
                timestamp_ns: now,
                exchange_event_timestamp_ns: None,
                trade_id: None,
                order_audit: None,
                error: None,
            })?;
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "hexmarket-live"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::hint::black_box;
    use std::time::Instant;

    fn insert(table: &mut OpenOrderTable, coid: &str, slot: OrderSlot) {
        let identity = table.prepare(coid, "ETH-USD").unwrap();
        table
            .insert_prepared(identity, Some("exchange-order-id"), Side::Buy, slot)
            .unwrap();
    }

    #[test]
    fn fixed_open_order_table_preserves_numeric_slot_and_backshifts_removals() {
        let mut table = OpenOrderTable::with_capacity(2);
        let first_slot = OrderSlot::with_generation(7, 3);
        insert(&mut table, "coid-a", first_slot);
        insert(&mut table, "coid-b", OrderSlot::with_generation(8, 4));
        assert_eq!(table.len(), 2);

        let removed = table.remove("coid-a").unwrap();
        assert_eq!(removed.order_slot, first_slot);
        assert_eq!(removed.symbol.as_str(), "ETH-USD");
        assert_eq!(table.len(), 1);

        insert(
            &mut table,
            "coid-c",
            OrderSlot::with_generation(9, 5),
        );
        assert!(table.contains("coid-b"));
        assert!(table.contains("coid-c"));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn fixed_open_order_table_fails_closed_before_identity_or_capacity_loss() {
        let mut table = OpenOrderTable::with_capacity(1);
        insert(&mut table, "coid-a", OrderSlot::with_generation(1, 1));
        let overflow = table.prepare("coid-b", "ETH-USD").unwrap();
        assert!(table
            .ensure_capacity_for(std::slice::from_ref(&overflow))
            .is_err());
        assert!(OpenOrderTable::with_capacity(1)
            .prepare(&"x".repeat(HEXMARKET_ORDER_IDENTITY_BYTES + 1), "ETH-USD")
            .is_err());
        assert!(OpenOrderTable::with_capacity(1)
            .prepare("coid", &"x".repeat(HEXMARKET_SYMBOL_BYTES + 1))
            .is_err());
    }

    #[test]
    fn fixed_open_order_lookup_reports_tail_latency_against_legacy_hashmap() {
        const ENTRIES: usize = 4_096;
        const EVENTS: usize = 20_000;
        let coids: Vec<String> = (0..ENTRIES)
            .map(|index| format!("latency-coid-{index:08}"))
            .collect();
        let mut fixed = OpenOrderTable::with_capacity(HEXMARKET_OPEN_ORDER_CAPACITY);
        for (index, coid) in coids.iter().enumerate() {
            insert(
                &mut fixed,
                coid,
                OrderSlot::with_generation(index as u16, 1),
            );
        }
        let legacy: HashMap<&str, usize> = coids
            .iter()
            .enumerate()
            .map(|(index, coid)| (coid.as_str(), index))
            .collect();

        let measure = |lookup: &mut dyn FnMut(&str) -> bool| {
            let mut samples = Vec::with_capacity(EVENTS);
            for event in 0..EVENTS {
                let coid = &coids[event % ENTRIES];
                let started = Instant::now();
                black_box(lookup(black_box(coid)));
                samples.push(started.elapsed().as_nanos() as u64);
            }
            samples.sort_unstable();
            let at = |parts_per_thousand: usize| {
                samples[(samples.len() - 1) * parts_per_thousand / 1_000]
            };
            (at(500), at(990), at(999), *samples.last().unwrap())
        };
        let mut fixed_lookup = |coid: &str| fixed.contains(coid);
        let mut legacy_lookup = |coid: &str| legacy.contains_key(coid);
        let fixed_tail = measure(&mut fixed_lookup);
        let legacy_tail = measure(&mut legacy_lookup);
        eprintln!(
            "hexmarket_open_order_lookup boundary=owner_identity_lookup events={EVENTS} entries={ENTRIES} capacity={} overflow=0 fixed_p50_ns={} fixed_p99_ns={} fixed_p999_ns={} fixed_max_ns={} legacy_p50_ns={} legacy_p99_ns={} legacy_p999_ns={} legacy_max_ns={}",
            HEXMARKET_OPEN_ORDER_CAPACITY,
            fixed_tail.0,
            fixed_tail.1,
            fixed_tail.2,
            fixed_tail.3,
            legacy_tail.0,
            legacy_tail.1,
            legacy_tail.2,
            legacy_tail.3,
        );
        assert_eq!(fixed.len(), ENTRIES);
    }
}
