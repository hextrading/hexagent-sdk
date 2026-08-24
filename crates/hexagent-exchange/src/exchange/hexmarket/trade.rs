use anyhow::{anyhow, Result};
use ed25519_dalek::SigningKey;
use log::{debug, warn};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;
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

/// Local record of an open order.
#[derive(Debug, Clone)]
struct TrackedOrder {
    exchange_order_id: Option<String>,
    symbol: String,
    side: Side,
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
    open_orders: HashMap<String, TrackedOrder>,
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
        let client = HexClient::new(HexClientConfig {
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
            open_orders: HashMap::new(),
        }
    }

    /// Create a parallel worker clone with its own HTTP client but shared state.
    pub fn clone_worker(&self) -> Self {
        let client = HexClient::new(HexClientConfig {
            api_url: self.shared.api_url_prefix.clone(),
        });
        if let (Some(pubkey), Some(creds)) = (&self.shared.pubkey, &self.shared.credentials) {
            client.set_credentials(pubkey, creds.clone());
        }
        Self {
            shared: Arc::clone(&self.shared),
            client,
            open_orders: HashMap::new(),
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
        if let Err(e) = self.check_rate_limit() {
            warn!(
                "[HexmarketTrade] RATE LIMITED: {} | coid={}",
                e, order.client_order_id
            );
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
                error: None,
            });
        }

        let params = self.build_order_params(order)?;
        let coid = &order.client_order_id;

        debug!(
            "[HexmarketTrade] Submit {} {} @ {} qty={} coid={}",
            order.symbol, params.side, params.price, params.quantity, coid,
        );

        match self.client.place_order(&params) {
            Ok(resp) => {
                debug!(
                    "[HexmarketTrade] Accepted: coid={} oid={}",
                    coid, resp.order_id
                );
                self.open_orders.insert(
                    coid.clone(),
                    TrackedOrder {
                        exchange_order_id: Some(resp.order_id.clone()),
                        symbol: order.symbol.clone(),
                        side: order.side,
                    },
                );
                Ok(OrderUpdate {
                    order_slot: Default::default(),
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
                warn!(
                    "[HexmarketTrade] REJECTED: {} | coid={}",
                    Self::format_sdk_error(e),
                    coid
                );
                Ok(OrderUpdate {
                    order_slot: Default::default(),
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
                    error: None,
                })
            }
        }
    }

    fn cancel_order(&mut self, exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        self.check_rate_limit()?;

        // Remove from local tracking if present (may not be tracked if synced from server)
        let tracked = self.open_orders.remove(client_order_id);

        debug!(
            "[HexmarketTrade] Cancel coid={} on {:?}",
            client_order_id, exchange
        );

        match self.client.cancel_order_by_client_id(client_order_id) {
            Ok(resp) => debug!("[HexmarketTrade] Cancelled: oid={}", resp.order_id),
            Err(e) => warn!(
                "[HexmarketTrade] Cancel failed coid={}: {}",
                client_order_id,
                Self::format_sdk_error(e)
            ),
        }

        Ok(OrderUpdate {
            order_slot: Default::default(),
            client_order_id: client_order_id.to_string(),
            exchange: Exchange::Hexmarket,
            symbol: tracked
                .as_ref()
                .map(|t| t.symbol.clone())
                .unwrap_or_default(),
            side: tracked.as_ref().map(|t| t.side).unwrap_or(Side::Buy),
            exchange_order_id: tracked.and_then(|t| t.exchange_order_id),
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
        exchange: Exchange,
        _symbol: &str,
        emit: &mut dyn FnMut(OrderUpdate) -> bool,
    ) -> Result<()> {
        self.check_rate_limit()?;
        let open_count = self.open_orders.len();
        debug!(
            "[HexmarketTrade] Cancel all on {:?} ({} open)",
            exchange, open_count
        );

        match self.client.cancel_all_orders(None, None) {
            Ok(resp) => debug!(
                "[HexmarketTrade] cancel_all: {} cancelled",
                resp.cancelled_count
            ),
            Err(e) => warn!(
                "[HexmarketTrade] cancel_all failed: {}",
                Self::format_sdk_error(e)
            ),
        }

        let now = now_ns();
        let mut delivery_open = true;
        for (coid, t) in self.open_orders.drain() {
            let update = OrderUpdate {
                order_slot: Default::default(),
                client_order_id: coid,
                exchange: Exchange::Hexmarket,
                symbol: t.symbol,
                side: t.side,
                exchange_order_id: t.exchange_order_id,
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
        }

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
        if let Err(e) = self.check_rate_limit() {
            warn!("[HexmarketTrade] RATE LIMITED batch place: {}", e);
            let now = now_ns();
            for o in orders {
                emit_fixed_update(out, OrderUpdate {
                    order_slot: Default::default(),
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
                    error: None,
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

        debug!(
            "[HexmarketTrade] Batch place {} orders for market {}",
            params_list.len(),
            market_id
        );

        match self.client.batch_place_orders(market_id, &params_list) {
            Ok(resp) => {
                let now = now_ns();
                let open = &mut self.open_orders;

                for (i, result) in resp.results.iter().enumerate() {
                    let order = &orders[i];
                    let coid = &order.client_order_id;
                    if let Some(ref err) = result.error {
                        warn!(
                            "[HexmarketTrade] Batch[{}] REJECTED coid={}: {}",
                            i, coid, err
                        );
                        emit_fixed_update(out, OrderUpdate {
                            order_slot: Default::default(),
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
                            timestamp_ns: now,
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: None,
                            error: None,
                        })?;
                    } else {
                        let oid = result.order_id.clone();
                        debug!(
                            "[HexmarketTrade] Batch[{}] OK coid={} oid={:?}",
                            i, coid, oid
                        );
                        open.insert(
                            coid.clone(),
                            TrackedOrder {
                                exchange_order_id: oid.clone(),
                                symbol: order.symbol.clone(),
                                side: order.side,
                            },
                        );
                        emit_fixed_update(out, OrderUpdate {
                            order_slot: Default::default(),
                            client_order_id: coid.clone(),
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
                }
                Ok(())
            }
            Err(e) => {
                warn!(
                    "[HexmarketTrade] Batch place FAILED: {}, fallback",
                    Self::format_sdk_error(e)
                );
                for order in orders {
                    emit_fixed_update(out, self.submit_order(order)?)?;
                }
                Ok(())
            }
        }
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

        // Remove from local tracking if present; proceed with API cancel regardless
        let mut to_cancel =
            arrayvec::ArrayVec::<(String, Option<TrackedOrder>), ORDER_BATCH_CAPACITY>::new();
        {
            let open = &mut self.open_orders;
            for coid in client_order_ids {
                let tracked = open.remove(coid);
                to_cancel
                    .try_push((coid.clone(), tracked))
                    .map_err(|_| anyhow!("Hexmarket cancel batch exceeds fixed capacity"))?;
            }
        }

        debug!(
            "[HexmarketTrade] Batch cancel {} orders for market {}",
            to_cancel.len(),
            market_id
        );

        let coid_refs: arrayvec::ArrayVec<&str, ORDER_BATCH_CAPACITY> =
            to_cancel.iter().map(|(coid, _)| coid.as_str()).collect();
        match self.client.batch_cancel_orders(market_id, &[], &coid_refs) {
            Ok(resp) => {
                for r in &resp.results {
                    if let Some(ref err) = r.error {
                        warn!(
                            "[HexmarketTrade] Batch cancel coid={:?}: {}",
                            r.client_order_id, err
                        );
                    }
                }
            }
            Err(e) => warn!(
                "[HexmarketTrade] Batch cancel failed: {}",
                Self::format_sdk_error(e)
            ),
        }
        drop(coid_refs);

        let now = now_ns();
        for (coid, t) in to_cancel {
            emit_fixed_update(out, OrderUpdate {
                order_slot: Default::default(),
                client_order_id: coid,
                exchange: Exchange::Hexmarket,
                symbol: t.as_ref().map(|o| o.symbol.clone()).unwrap_or_default(),
                side: t.as_ref().map(|o| o.side).unwrap_or(Side::Buy),
                exchange_order_id: t.and_then(|o| o.exchange_order_id),
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
        self.check_rate_limit()?;
        // Remove from local tracking; proceed regardless of whether tracked locally
        let mut cancel_tracked =
            arrayvec::ArrayVec::<(String, Option<TrackedOrder>), ORDER_BATCH_CAPACITY>::new();
        {
            let open = &mut self.open_orders;
            for coid in cancel_client_order_ids {
                let tracked = open.remove(coid);
                cancel_tracked
                    .try_push((coid.clone(), tracked))
                    .map_err(|_| anyhow!("Hexmarket update cancel batch exceeds fixed capacity"))?;
            }
        }

        let mut params_list = arrayvec::ArrayVec::<PlaceOrderParams, ORDER_BATCH_CAPACITY>::new();
        for order in place_orders {
            params_list
                .try_push(self.build_order_params(order)?)
                .map_err(|_| anyhow!("Hexmarket update place batch exceeds fixed capacity"))?;
        }

        let cancel_refs: arrayvec::ArrayVec<&str, ORDER_BATCH_CAPACITY> = cancel_tracked
            .iter()
            .map(|(coid, _)| coid.as_str())
            .collect();

        debug!(
            "[HexmarketTrade] Batch update market {}: cancel={} place={}",
            market_id,
            cancel_refs.len(),
            params_list.len()
        );

        let response = self
            .client
            .batch_update_orders(market_id, &[], &params_list, Some(&cancel_refs));
        drop(cancel_refs);
        match response {
            Ok(resp) => {
                let now = now_ns();

                for r in &resp.cancel_results {
                    if let Some(ref err) = r.error {
                        warn!(
                            "[HexmarketTrade] Update cancel coid={:?}: {}",
                            r.client_order_id, err
                        );
                    }
                }
                for (coid, t) in &cancel_tracked {
                    emit_fixed_update(out, OrderUpdate {
                        order_slot: Default::default(),
                        client_order_id: coid.clone(),
                        exchange: Exchange::Hexmarket,
                        symbol: t.as_ref().map(|o| o.symbol.clone()).unwrap_or_default(),
                        side: t.as_ref().map(|o| o.side).unwrap_or(Side::Buy),
                        exchange_order_id: t.as_ref().and_then(|o| o.exchange_order_id.clone()),
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
                    })?;
                }

                let open = &mut self.open_orders;
                for (i, result) in resp.place_results.iter().enumerate() {
                    let order = &place_orders[i];
                    let coid = &order.client_order_id;
                    if let Some(ref err) = result.error {
                        warn!(
                            "[HexmarketTrade] Update place[{}] REJECTED coid={}: {}",
                            i, coid, err
                        );
                        emit_fixed_update(out, OrderUpdate {
                            order_slot: Default::default(),
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
                            timestamp_ns: now,
                            exchange_event_timestamp_ns: None,
                            trade_id: None,
                            order_audit: None,
                            error: None,
                        })?;
                    } else {
                        let oid = result.order_id.clone();
                        open.insert(
                            coid.clone(),
                            TrackedOrder {
                                exchange_order_id: oid.clone(),
                                symbol: order.symbol.clone(),
                                side: order.side,
                            },
                        );
                        emit_fixed_update(out, OrderUpdate {
                            order_slot: Default::default(),
                            client_order_id: coid.clone(),
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
                }
                Ok(())
            }
            Err(e) => {
                warn!(
                    "[HexmarketTrade] Batch update FAILED: {}, fallback",
                    Self::format_sdk_error(e)
                );
                {
                    let open = &mut self.open_orders;
                    for (coid, tracked) in cancel_tracked {
                        if let Some(t) = tracked {
                            open.insert(coid, t);
                        }
                    }
                }
                if !cancel_client_order_ids.is_empty() {
                    self.batch_cancel_orders_into(
                        Exchange::Hexmarket,
                        market_id,
                        cancel_client_order_ids,
                        out,
                    )?;
                }
                if !place_orders.is_empty() {
                    self.batch_submit_orders_into(market_id, place_orders, out)?;
                }
                Ok(())
            }
        }
    }

    fn name(&self) -> &str {
        "hexmarket-live"
    }
}
