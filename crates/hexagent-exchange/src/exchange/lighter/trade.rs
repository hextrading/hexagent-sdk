//! Lighter order execution — the `ExchangeTrade` impl.
//!
//! Signed L2 transactions are POSTed (form-encoded `tx_type` + `tx_info`) to
//! `/api/v1/sendTx`. `code == 200` means the tx passed syntax checks and was
//! queued — execution (and maker rests / fills) is reported asynchronously
//! via the user feed, so a successful place ack maps to `Accepted`.
//!
//! The strategy's `client_order_id` is a UUID; Lighter wants a 48-bit
//! `ClientOrderIndex`, so we derive one from the UUID's low 48 bits and keep
//! the `uuid → (market, coi)` map for cancels (mirrors Hyperliquid's cloid
//! bookkeeping). Cancels go through `Index = client_order_index`, which the
//! sequencer resolves for both resting and in-flight orders (our cancel tx
//! is sequenced after the create by nonce order).
//!
//! Nonces are per-(account, api-key) and strictly sequential: seeded from
//! `nextNonce` once, incremented locally per tx, and resynced from REST after
//! any send failure (a rejected-but-200 tx still burns its nonce; a transport
//! error may not — resync is the only safe recovery either way).

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use log::{debug, warn};
use serde::Deserialize;

use crate::async_rt;
use crate::types::{
    Exchange, OrderRequest, OrderStatus, OrderType, OrderUpdate, Side,
};

use super::auth::LighterAuth;
use super::info::{fetch_next_nonce, LighterMeta};
use super::signer::{
    CreateOrderParams, SignedTx, ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET,
    TIF_GOOD_TILL_TIME, TIF_IMMEDIATE_OR_CANCEL, TIF_POST_ONLY,
};

/// Tx deadline: how far in the future `ExpiredAt` (ms) is set.
const TX_EXPIRY_MS: i64 = 10 * 60 * 1000;
/// Resting-order expiry for GTT/post-only orders (28 days, matching the
/// official SDK default; quotes are replaced long before this).
const ORDER_EXPIRY_MS: i64 = 28 * 24 * 60 * 60 * 1000;
/// Market-order slippage (fraction) — worst acceptable price offset from the
/// current touch, encoded into the IOC's limit price.
const MARKET_SLIPPAGE: f64 = 0.02;

pub struct LighterTrade {
    auth: LighterAuth,
    meta: LighterMeta,
    /// Reserved for multi-instance tagging (single-account in v1).
    #[allow(dead_code)]
    instance_id: String,
    /// client_order_id (uuid) → (market_index, client_order_index).
    coid_index: HashMap<String, (i16, i64)>,
    /// Client order indexes placed by the PREVIOUS `batch_update_orders`
    /// cycle, per symbol — cancelled at the start of the next cycle
    /// (place-then-cancel, race-free; same scheme as Hyperliquid).
    prev_cois: HashMap<String, Vec<i64>>,
    /// Next nonce to use; `None` until seeded from REST.
    next_nonce: Option<i64>,
}

impl LighterTrade {
    pub fn new(auth: LighterAuth, meta: LighterMeta, instance_id: &str) -> Self {
        Self {
            auth,
            meta,
            instance_id: instance_id.to_string(),
            coid_index: HashMap::new(),
            prev_cois: HashMap::new(),
            next_nonce: None,
        }
    }

    fn now_ms() -> i64 {
        (crate::types::now_ns() / 1_000_000) as i64
    }

    /// Take the next nonce, seeding from REST on first use.
    fn take_nonce(&mut self) -> Result<i64> {
        if self.next_nonce.is_none() {
            let n = fetch_next_nonce(
                &self.auth.rest_base(),
                self.auth.account_index(),
                self.auth.api_key_index(),
            )?;
            self.next_nonce = Some(n);
        }
        let n = self.next_nonce.unwrap();
        self.next_nonce = Some(n + 1);
        Ok(n)
    }

    /// Drop the local nonce so the next tx reseeds from REST.
    fn invalidate_nonce(&mut self) {
        self.next_nonce = None;
    }

    /// POST a signed tx to `/api/v1/sendTx`. Any failure (transport, HTTP,
    /// app-level code) invalidates the local nonce sequence.
    fn send_tx(&mut self, tx: &SignedTx) -> Result<SendTxResponse> {
        let url = format!("{}/api/v1/sendTx", self.auth.rest_base());
        let form = [
            ("tx_type", tx.tx_type.to_string()),
            ("tx_info", tx.tx_info.clone()),
        ];
        let client = async_rt::http_client_auto();
        let result = async_rt::block_on_runtime(async move {
            let resp = client
                .post(&url)
                .form(&form)
                .send()
                .await
                .map_err(|e| anyhow!("POST {}: {}", url, e))?;
            let status = resp.status();
            let text = resp.text().await.map_err(|e| anyhow!("read body: {}", e))?;
            if !status.is_success() {
                return Err(anyhow!("POST {} -> {}: {}", url, status, text));
            }
            serde_json::from_str::<SendTxResponse>(&text)
                .map_err(|e| anyhow!("parse sendTx response: {} — body: {}", e, text))
        });
        match result {
            Ok(resp) if resp.code == 200 => Ok(resp),
            Ok(resp) => {
                // App-level reject after a 200-coded envelope is NOT possible
                // (code is the envelope); a non-200 code here means the tx was
                // rejected pre-sequencer — nonce state is ambiguous, resync.
                self.invalidate_nonce();
                Err(anyhow!(
                    "sendTx rejected: code={} message={}",
                    resp.code,
                    resp.message.unwrap_or_default()
                ))
            }
            Err(e) => {
                self.invalidate_nonce();
                Err(e)
            }
        }
    }

    fn market_meta(&self, symbol: &str) -> Result<(i16, u32, u32)> {
        let m = self
            .meta
            .market(symbol)
            .ok_or_else(|| anyhow!("lighter: unknown symbol `{}`", symbol))?;
        Ok((m.market_id, m.price_decimals, m.size_decimals))
    }

    /// Aggressive limit price for a market order: cross the current touch by
    /// `slippage` (buy → best ask ×(1+s), sell → best bid ×(1−s)).
    fn market_price(&self, symbol: &str, market_id: i16, side: Side, slippage: f64) -> Result<f64> {
        let book = fetch_best_prices(&self.auth.rest_base(), market_id)?;
        let px = match side {
            Side::Buy => book.best_ask.map(|a| a * (1.0 + slippage)),
            Side::Sell => book.best_bid.map(|b| b * (1.0 - slippage)),
        };
        px.filter(|p| *p > 0.0)
            .ok_or_else(|| anyhow!("lighter: empty book for market order on {}", symbol))
    }
}

/// Derive the 48-bit `ClientOrderIndex` from a UUID `client_order_id`
/// (low 48 bits of the hex digits; must be ≥ 1). Non-UUID ids hash to the
/// same range so cancels stay consistent for any id shape.
fn coid_to_index(client_order_id: &str) -> i64 {
    let hex: String = client_order_id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    let v = if hex.len() >= 12 {
        i64::from_str_radix(&hex[hex.len() - 12..], 16).unwrap_or(0)
    } else {
        // Fallback: FNV-1a over the raw id.
        let mut h: u64 = 0xcbf29ce484222325;
        for b in client_order_id.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        (h & ((1 << 48) - 1)) as i64
    };
    v.max(1) // 0 is NilClientOrderIndex
}

/// Scale a float to integer wire units with `decimals`.
fn scale(v: f64, decimals: u32) -> i64 {
    (v * 10f64.powi(decimals as i32)).round() as i64
}

impl super::super::ExchangeTrade for LighterTrade {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        let symbol = order.symbol.clone();
        let (market_id, px_dec, sz_dec) = self.market_meta(&symbol)?;

        let is_market = order.order_type == OrderType::Market;
        let price = match order.price {
            Some(p) => p,
            None if is_market => self.market_price(&symbol, market_id, order.side, MARKET_SLIPPAGE)?,
            None => return Err(anyhow!("lighter: limit order without price")),
        };

        let (order_type, tif, order_expiry) = match order.order_type {
            OrderType::Market => (ORDER_TYPE_MARKET, TIF_IMMEDIATE_OR_CANCEL, 0),
            OrderType::Fak | OrderType::Fok => (ORDER_TYPE_LIMIT, TIF_IMMEDIATE_OR_CANCEL, 0),
            OrderType::LimitMaker => {
                (ORDER_TYPE_LIMIT, TIF_POST_ONLY, Self::now_ms() + ORDER_EXPIRY_MS)
            }
            OrderType::Limit => {
                if order.post_only {
                    (ORDER_TYPE_LIMIT, TIF_POST_ONLY, Self::now_ms() + ORDER_EXPIRY_MS)
                } else {
                    (ORDER_TYPE_LIMIT, TIF_GOOD_TILL_TIME, Self::now_ms() + ORDER_EXPIRY_MS)
                }
            }
        };

        let coi = coid_to_index(&order.client_order_id);
        let nonce = self.take_nonce()?;
        let params = CreateOrderParams {
            market_index: market_id,
            client_order_index: coi,
            base_amount: scale(order.quantity, sz_dec),
            price: scale(price, px_dec).clamp(1, u32::MAX as i64) as u32,
            is_ask: matches!(order.side, Side::Sell),
            order_type,
            time_in_force: tif,
            reduce_only: false,
            trigger_price: 0,
            order_expiry,
            nonce,
            expired_at: Self::now_ms() + TX_EXPIRY_MS,
        };
        let tx = self.auth.signer.sign_create_order(&params)?;
        let resp = self.send_tx(&tx)?;

        self.coid_index
            .insert(order.client_order_id.clone(), (market_id, coi));
        debug!(
            "[Lighter] placed coi={} {} {:?} px={} qty={} tx_hash={}",
            coi, symbol, order.side, price, order.quantity,
            resp.tx_hash.as_deref().unwrap_or(&tx.tx_hash),
        );

        Ok(OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange: Exchange::Lighter,
            symbol,
            side: order.side,
            exchange_order_id: Some(coi.to_string()),
            status: OrderStatus::Accepted,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: order.quantity,
            avg_fill_price: 0.0,
            timestamp_ns: crate::types::now_ns(),
            trade_id: None,
            error: None,
        })
    }

    fn cancel_order(&mut self, _exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        // Unknown coid = already gone / never placed this session. Benign
        // no-op (mirrors the Hyperliquid path) so one stale id can't fail a
        // whole cancel+replace batch.
        let (market_id, coi) = match self.coid_index.get(client_order_id).copied() {
            Some(v) => v,
            None => {
                debug!("[Lighter] cancel for unknown coid {} — no-op", client_order_id);
                return Ok(cancel_update(client_order_id, true, None));
            }
        };
        let nonce = self.take_nonce()?;
        let tx = self.auth.signer.sign_cancel_order(
            market_id,
            coi,
            nonce,
            Self::now_ms() + TX_EXPIRY_MS,
        )?;
        let result = self.send_tx(&tx);
        self.coid_index.remove(client_order_id);
        match result {
            Ok(_) => Ok(cancel_update(client_order_id, true, None)),
            Err(e) => Ok(cancel_update(client_order_id, false, Some(e.to_string()))),
        }
    }

    /// Race-free replace: place the new quotes first, then cancel the
    /// PREVIOUS cycle's client-order-indexes. Cancels are sequenced after
    /// the creates (nonce order), so this never leaves a naked book; the
    /// resting set briefly doubles for ~one sequencer round.
    fn batch_update_orders(
        &mut self,
        _exchange: Exchange,
        market_id: &str,
        _cancel_client_order_ids: &[String],
        place_orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        let symbol = place_orders
            .first()
            .map(|o| o.symbol.clone())
            .unwrap_or_else(|| market_id.to_string());

        // 1. place the new quotes.
        let mut updates = Vec::with_capacity(place_orders.len());
        let mut new_cois = Vec::new();
        for o in place_orders {
            let u = self.submit_order(o)?;
            if let Some(coi) = u.exchange_order_id.as_ref().and_then(|s| s.parse::<i64>().ok()) {
                new_cois.push(coi);
            }
            updates.push(u);
        }

        // 2. cancel the previous cycle's orders (best-effort).
        let prev = self.prev_cois.remove(&symbol).unwrap_or_default();
        if !prev.is_empty() {
            if let Ok((mid, _, _)) = self.market_meta(&symbol) {
                for coi in &prev {
                    let nonce = match self.take_nonce() {
                        Ok(n) => n,
                        Err(e) => {
                            warn!("[Lighter] replace-cancel nonce fetch failed: {}", e);
                            break;
                        }
                    };
                    match self
                        .auth
                        .signer
                        .sign_cancel_order(mid, *coi, nonce, Self::now_ms() + TX_EXPIRY_MS)
                        .and_then(|tx| self.send_tx(&tx))
                    {
                        Ok(_) => {}
                        Err(e) => warn!("[Lighter] replace cancel coi={} failed: {}", coi, e),
                    }
                }
            }
        }

        // 3. remember this cycle's indexes for the next replace.
        if !new_cois.is_empty() {
            self.prev_cois.insert(symbol, new_cois);
        }
        Ok(updates)
    }

    /// Immediate cancel-all (all markets for this account/api-key — Lighter's
    /// cancel-all is account-scoped, which matches the single-market
    /// deployments this venue is used for).
    fn cancel_all(&mut self, _exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        self.prev_cois.remove(symbol);
        let nonce = self.take_nonce()?;
        let tx = self
            .auth
            .signer
            .sign_cancel_all(nonce, Self::now_ms() + TX_EXPIRY_MS)?;
        self.send_tx(&tx)?;
        let coids: Vec<String> = self.coid_index.keys().cloned().collect();
        self.coid_index.clear();
        Ok(coids
            .iter()
            .map(|c| cancel_update(c, true, None))
            .collect())
    }

    fn name(&self) -> &str {
        "lighter"
    }
}

fn cancel_update(client_order_id: &str, ok: bool, err: Option<String>) -> OrderUpdate {
    OrderUpdate {
        client_order_id: client_order_id.to_string(),
        exchange: Exchange::Lighter,
        symbol: String::new(),
        side: Side::Buy, // not meaningful for a cancel ack
        exchange_order_id: None,
        status: if ok { OrderStatus::Cancelled } else { OrderStatus::Rejected },
        liquidity: None,
        filled_quantity: 0.0,
        remaining_quantity: 0.0,
        avg_fill_price: 0.0,
        timestamp_ns: crate::types::now_ns(),
        trade_id: None,
        error: err,
    }
}

// ── sendTx response ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SendTxResponse {
    pub code: i32,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub tx_hash: Option<String>,
}

// ── best-price snapshot (market-order pricing) ────────────────────

#[derive(Debug, Default)]
struct BestPrices {
    best_bid: Option<f64>,
    best_ask: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct ObOrder {
    price: String,
}

#[derive(Debug, Deserialize)]
struct OrderBookOrdersResponse {
    #[serde(default)]
    asks: Vec<ObOrder>,
    #[serde(default)]
    bids: Vec<ObOrder>,
}

fn fetch_best_prices(rest_base: &str, market_id: i16) -> Result<BestPrices> {
    let resp: OrderBookOrdersResponse = super::info::get_json(format!(
        "{}/api/v1/orderBookOrders?market_id={}&limit=1",
        rest_base, market_id
    ))?;
    Ok(BestPrices {
        best_bid: resp.bids.first().and_then(|o| o.price.parse().ok()),
        best_ask: resp.asks.first().and_then(|o| o.price.parse().ok()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coid_index_from_uuid() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        // low 48 bits of the 32 hex digits = last 12 chars "446655440000"
        assert_eq!(coid_to_index(uuid), 0x446655440000);
        // short ids fall back to FNV within 48 bits, never 0
        let v = coid_to_index("x");
        assert!(v >= 1 && v <= super::super::signer::MAX_CLIENT_ORDER_INDEX);
    }

    #[test]
    fn scaling() {
        assert_eq!(scale(62835.6, 1), 628356);
        assert_eq!(scale(0.0002, 5), 20);
        assert_eq!(scale(0.00031, 5), 31);
    }
}
