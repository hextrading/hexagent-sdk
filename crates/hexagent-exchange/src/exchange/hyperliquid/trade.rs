//! Hyperliquid order execution — the `ExchangeTrade` impl.
//!
//! Orders are placed/cancelled via signed L1 actions POSTed to `/exchange`
//! (see [`super::signer`]). The strategy's `client_order_id` is a UUID (128
//! bits), mapped 1:1 to Hyperliquid's `cloid` (`0x` + 32 hex), so cancels go
//! through `cancelByCloid` without needing the exchange `oid`.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use log::{debug, warn};
use serde::Deserialize;
use serde_json::json;

use crate::async_rt;
use crate::types::{
    Exchange, OrderRequest, OrderStatus, OrderType, OrderUpdate, Side,
};

use super::auth::HlAuth;
use super::info::HlMeta;
use super::signer::{
    float_to_wire, sign_l1_action, CancelAction, CancelByCloidAction,
    CancelCloidWire, CancelWire, LimitWire, OrderAction, OrderTypeWire, OrderWire,
};

pub struct HyperliquidTrade {
    auth: HlAuth,
    meta: HlMeta,
    /// Reserved for multi-instance cloid tagging (single-account in v1).
    #[allow(dead_code)]
    instance_id: String,
    /// client_order_id → asset index (populated on submit; used by cancels).
    coid_asset: HashMap<String, u32>,
    /// Exchange oids of the orders placed by the PREVIOUS `batch_update_orders`
    /// cycle, per coin. Cancelled (by oid — authoritative, race-free) at the
    /// start of the next cycle. Replaces fragile per-cloid replace bookkeeping.
    prev_oids: HashMap<String, Vec<u64>>,
    /// Monotonic nonce (ms) — strictly increasing per process/account.
    last_nonce: u64,
}

impl HyperliquidTrade {
    pub fn new(auth: HlAuth, meta: HlMeta, instance_id: &str) -> Self {
        Self {
            auth,
            meta,
            instance_id: instance_id.to_string(),
            coid_asset: HashMap::new(),
            prev_oids: HashMap::new(),
            last_nonce: 0,
        }
    }

    fn next_nonce(&mut self) -> u64 {
        let now_ms = crate::types::now_ns() / 1_000_000;
        let n = now_ms.max(self.last_nonce + 1);
        self.last_nonce = n;
        n
    }

    fn vault(&self) -> Option<&str> {
        None // master/EOA account signing; subaccounts/vaults not used
    }

    /// Sign `action` and POST `{action, nonce, signature, vaultAddress}`.
    fn send_signed<T: serde::Serialize>(&mut self, action: &T) -> Result<ExchangeResponse> {
        let nonce = self.next_nonce();
        let is_mainnet = self.auth.network.is_mainnet();
        let sig = sign_l1_action(&self.auth.key, action, self.vault(), nonce, None, is_mainnet)?;
        let body = json!({
            "action": action,
            "nonce": nonce,
            "signature": { "r": sig.r, "s": sig.s, "v": sig.v },
            "vaultAddress": self.vault(),
        });
        let url = self.auth.exchange_url();
        // ALPN-negotiating client — see info.rs: HL REST rejects h2 prior
        // knowledge, so the Polymarket-tuned `http_client_fast` pool fails.
        let client = async_rt::http_client_auto();
        let text = async_rt::block_on_runtime(async move {
            let resp = client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
                .map_err(|e| anyhow!("POST {}: {}", url, e))?;
            let status = resp.status();
            let text = resp.text().await.map_err(|e| anyhow!("read body: {}", e))?;
            if !status.is_success() {
                return Err(anyhow!("POST {} -> {}: {}", url, status, text));
            }
            Ok::<String, anyhow::Error>(text)
        })?;
        serde_json::from_str::<ExchangeResponse>(&text)
            .map_err(|e| anyhow!("parse /exchange response: {} — body: {}", e, text))
    }

    fn asset_for(&self, coin: &str) -> Result<u32> {
        self.meta
            .asset_index(coin)
            .ok_or_else(|| anyhow!("hyperliquid: unknown coin/asset `{}`", coin))
    }

    /// Aggressive limit price for a market order: cross the current touch by
    /// `slippage` so an IOC fills immediately. Buy → best ask ×(1+slip),
    /// sell → best bid ×(1−slip). Fetches a fresh l2Book snapshot.
    fn market_price(&self, coin: &str, side: Side, slippage: f64) -> Result<f64> {
        let book = super::info::fetch_l2_book(&self.auth.info_url(), coin)?;
        if book.levels.len() != 2 {
            return Err(anyhow!("hyperliquid: malformed l2Book for market order"));
        }
        let (bids, asks) = (&book.levels[0], &book.levels[1]);
        let px = match side {
            Side::Buy => asks.first().and_then(|l| l.px.parse::<f64>().ok()).map(|a| a * (1.0 + slippage)),
            Side::Sell => bids.first().and_then(|l| l.px.parse::<f64>().ok()).map(|b| b * (1.0 - slippage)),
        };
        px.filter(|p| *p > 0.0)
            .ok_or_else(|| anyhow!("hyperliquid: empty book for market order on {}", coin))
    }
}

/// Market-order slippage (fraction) — cross the touch by this much so the IOC
/// fills through the top of book. 2% is ample for perps like BTC/ETH.
const MARKET_SLIPPAGE: f64 = 0.02;

impl super::super::ExchangeTrade for HyperliquidTrade {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        let coin = order.symbol.clone();
        let asset = self.asset_for(&coin)?;
        // Hyperliquid has no native market order — a `Market` request (price
        // unset) is emulated as an aggressive IOC limit at the current best
        // price ± slippage, guaranteeing an immediate cross. Limit/maker orders
        // still require an explicit price.
        let price = match order.price {
            Some(p) => p,
            None if order.order_type == OrderType::Market => {
                self.market_price(&coin, order.side, MARKET_SLIPPAGE)?
            }
            None => return Err(anyhow!("hyperliquid: limit order without price")),
        };
        let sz_dec = self.meta.sz_decimals(&coin).unwrap_or(4);
        let px_str = float_to_wire(round_price(price, sz_dec));
        let sz_str = float_to_wire(round_size(order.quantity, sz_dec));
        let is_buy = matches!(order.side, Side::Buy);
        let tif = tif_for(order.order_type, order.post_only);
        let cloid = coid_to_cloid(&order.client_order_id);

        self.coid_asset.insert(order.client_order_id.clone(), asset);

        let action = OrderAction {
            ty: "order".to_string(),
            orders: vec![OrderWire {
                a: asset,
                b: is_buy,
                p: px_str,
                s: sz_str,
                r: false,
                t: OrderTypeWire { limit: LimitWire { tif } },
                c: cloid,
            }],
            grouping: "na".to_string(),
            builder: None,
        };

        let resp = self.send_signed(&action)?;
        let status = resp.first_status()?;
        Ok(status.into_order_update(order, order.quantity))
    }

    fn cancel_order(&mut self, _exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        // Unknown cloid = already gone / never rested. Treat as a benign no-op
        // (like an "already cancelled" order) rather than erroring — otherwise a
        // single stale cloid fails the whole cancel+replace batch and orphans
        // the new quotes. Mirrors polymarket's benign "already canceled" path.
        let asset = match self.coid_asset.get(client_order_id).copied() {
            Some(a) => a,
            None => {
                debug!("[Hyperliquid] cancel for unknown cloid {} — no-op", client_order_id);
                return Ok(cancel_update(client_order_id, true, None));
            }
        };
        let cloid = coid_to_cloid(client_order_id)
            .ok_or_else(|| anyhow!("hyperliquid: client_order_id not a uuid: {}", client_order_id))?;
        let action = CancelByCloidAction {
            ty: "cancelByCloid".to_string(),
            cancels: vec![CancelCloidWire { asset, cloid }],
        };
        let resp = self.send_signed(&action)?;
        let ok = resp.cancel_ok();
        // Prune so the map doesn't grow unbounded across replace cycles.
        self.coid_asset.remove(client_order_id);
        Ok(cancel_update(client_order_id, ok, resp.cancel_error()))
    }

    /// Race-free replace: place the new quotes first (capturing their oids),
    /// then cancel the PREVIOUS cycle's oids by oid (authoritative — unlike
    /// cloid bookkeeping, cancelling a concrete oid can't race a not-yet-placed
    /// order or silently no-op). The `cancel_client_order_ids` cloid list from
    /// the strategy is intentionally ignored in favour of this scheme.
    /// Place-then-cancel briefly doubles the resting set (~one RTT) but never
    /// leaves a naked book or leaks orders across cycles.
    fn batch_update_orders(
        &mut self,
        _exchange: Exchange,
        market_id: &str,
        _cancel_client_order_ids: &[String],
        place_orders: &[OrderRequest],
    ) -> Result<Vec<OrderUpdate>> {
        let coin = place_orders
            .first()
            .map(|o| o.symbol.clone())
            .unwrap_or_else(|| market_id.to_string());

        // 1. place the new quotes, collect their resting oids.
        let mut updates = Vec::with_capacity(place_orders.len());
        let mut new_oids = Vec::new();
        for o in place_orders {
            let u = self.submit_order(o)?;
            if let Some(oid) = u.exchange_order_id.as_ref().and_then(|s| s.parse::<u64>().ok()) {
                new_oids.push(oid);
            }
            updates.push(u);
        }

        // 2. cancel the previous cycle's oids (best-effort, single action).
        let prev = self.prev_oids.remove(&coin).unwrap_or_default();
        if !prev.is_empty() {
            if let Ok(asset) = self.asset_for(&coin) {
                let action = CancelAction {
                    ty: "cancel".to_string(),
                    cancels: prev.iter().map(|o| CancelWire { a: asset, o: *o }).collect(),
                };
                if let Err(e) = self.send_signed(&action) {
                    warn!("[Hyperliquid] replace cancel of {} prev oids failed: {}", prev.len(), e);
                }
            }
        }

        // 3. remember this cycle's oids for the next replace.
        if !new_oids.is_empty() {
            self.prev_oids.insert(coin, new_oids);
        }
        Ok(updates)
    }

    fn cancel_all(&mut self, _exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        let asset = self.asset_for(symbol)?;
        self.prev_oids.remove(symbol); // authoritative sweep supersedes tracked oids
        // Query open orders for the account, keep this coin's oids.
        let open = fetch_open_oids(&self.auth, symbol)?;
        if open.is_empty() {
            return Ok(Vec::new());
        }
        let action = CancelAction {
            ty: "cancel".to_string(),
            cancels: open.iter().map(|(oid, _)| CancelWire { a: asset, o: *oid }).collect(),
        };
        let resp = self.send_signed(&action)?;
        let ok = resp.cancel_ok();
        let mut updates = Vec::new();
        for (_oid, cloid) in open {
            let coid = cloid.unwrap_or_default();
            updates.push(cancel_update(&coid, ok, None));
        }
        if !ok {
            warn!("[Hyperliquid] cancel_all({}) returned non-success", symbol);
        }
        Ok(updates)
    }

    fn name(&self) -> &str {
        "hyperliquid"
    }
}

// ════════════════════════════════════════════════════════════════
// Response parsing
// ════════════════════════════════════════════════════════════════

/// Top-level `/exchange` reply. On success `response` is
/// `{"type":"order|cancel","data":{"statuses":[...]}}`; on failure it is a
/// plain error string. Kept as a `Value` so the error case parses cleanly.
#[derive(Debug, Deserialize)]
struct ExchangeResponse {
    status: String,
    #[serde(default)]
    response: serde_json::Value,
}

/// One status per order/cancel in the batch. Untagged: exactly one of the
/// fields is present.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StatusEntry {
    Resting { resting: RestingStatus },
    Filled { filled: FilledStatus },
    Error { error: String },
    /// Cancel batch returns plain strings: "success" or an error message.
    Str(String),
}

#[derive(Debug, Deserialize)]
struct RestingStatus {
    oid: u64,
    #[allow(dead_code)]
    #[serde(default)]
    cloid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FilledStatus {
    #[serde(rename = "totalSz")]
    total_sz: String,
    #[serde(rename = "avgPx")]
    avg_px: String,
    oid: u64,
}

impl ExchangeResponse {
    /// Error string when `status != "ok"` (response is a bare string).
    fn err_string(&self) -> Option<String> {
        if self.status == "ok" {
            None
        } else {
            Some(match &self.response {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })
        }
    }

    fn statuses(&self) -> Vec<StatusEntry> {
        self.response
            .get("data")
            .and_then(|d| d.get("statuses"))
            .and_then(|s| serde_json::from_value::<Vec<StatusEntry>>(s.clone()).ok())
            .unwrap_or_default()
    }

    fn first_status(&self) -> Result<StatusEntry> {
        if let Some(e) = self.err_string() {
            return Err(anyhow!("/exchange err: {}", e));
        }
        self.statuses()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("/exchange: empty statuses"))
    }

    fn cancel_ok(&self) -> bool {
        if self.status != "ok" {
            return false;
        }
        let st = self.statuses();
        !st.is_empty()
            && st.iter().all(|s| matches!(s, StatusEntry::Str(v) if v == "success"))
    }

    fn cancel_error(&self) -> Option<String> {
        if let Some(e) = self.err_string() {
            return Some(e);
        }
        self.statuses().iter().find_map(|s| match s {
            StatusEntry::Str(v) if v != "success" => Some(v.clone()),
            StatusEntry::Error { error } => Some(error.clone()),
            _ => None,
        })
    }
}

impl StatusEntry {
    fn into_order_update(&self, order: &OrderRequest, req_qty: f64) -> OrderUpdate {
        let base = |status: OrderStatus,
                    oid: Option<String>,
                    filled: f64,
                    avg: f64,
                    err: Option<String>| OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange: Exchange::Hyperliquid,
            symbol: order.symbol.clone(),
            side: order.side,
            exchange_order_id: oid,
            status,
            liquidity: None,
            filled_quantity: filled,
            remaining_quantity: (req_qty - filled).max(0.0),
            avg_fill_price: avg,
            timestamp_ns: crate::types::now_ns(),
            trade_id: None,
            error: err,
        };
        match self {
            StatusEntry::Resting { resting } => {
                base(OrderStatus::Accepted, Some(resting.oid.to_string()), 0.0, 0.0, None)
            }
            StatusEntry::Filled { filled } => {
                let sz: f64 = filled.total_sz.parse().unwrap_or(0.0);
                let avg: f64 = filled.avg_px.parse().unwrap_or(0.0);
                let status = if sz + 1e-12 >= req_qty {
                    OrderStatus::Filled
                } else {
                    OrderStatus::PartiallyFilled
                };
                base(status, Some(filled.oid.to_string()), sz, avg, None)
            }
            StatusEntry::Error { error } => {
                base(OrderStatus::Rejected, None, 0.0, 0.0, Some(error.clone()))
            }
            StatusEntry::Str(v) => {
                // Unexpected on a place; treat non-"success" as rejection.
                if v == "success" {
                    base(OrderStatus::Accepted, None, 0.0, 0.0, None)
                } else {
                    base(OrderStatus::Rejected, None, 0.0, 0.0, Some(v.clone()))
                }
            }
        }
    }
}

fn cancel_update(client_order_id: &str, ok: bool, err: Option<String>) -> OrderUpdate {
    OrderUpdate {
        client_order_id: client_order_id.to_string(),
        exchange: Exchange::Hyperliquid,
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

// ════════════════════════════════════════════════════════════════
// Open-orders query (for cancel_all)
// ════════════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
struct OpenOrder {
    coin: String,
    oid: u64,
    #[serde(default)]
    cloid: Option<String>,
}

/// Returns `(oid, cloid?)` for the account's open orders on `coin`.
fn fetch_open_oids(auth: &HlAuth, coin: &str) -> Result<Vec<(u64, Option<String>)>> {
    let orders: Vec<OpenOrder> = super::info::post_info(
        &auth.info_url(),
        json!({ "type": "openOrders", "user": auth.account_address }),
    )?;
    Ok(orders
        .into_iter()
        .filter(|o| o.coin == coin)
        .map(|o| (o.oid, o.cloid))
        .collect())
}

// ════════════════════════════════════════════════════════════════
// Wire helpers
// ════════════════════════════════════════════════════════════════

/// Map an `OrderType` (+ post_only) to a Hyperliquid limit `tif`.
fn tif_for(order_type: OrderType, post_only: bool) -> String {
    match order_type {
        OrderType::LimitMaker => "Alo".to_string(),
        OrderType::Fak | OrderType::Fok | OrderType::Market => "Ioc".to_string(),
        OrderType::Limit => {
            if post_only {
                "Alo".to_string()
            } else {
                "Gtc".to_string()
            }
        }
    }
}

/// Convert a UUID `client_order_id` to a Hyperliquid `cloid` (`0x` + 32 hex).
/// Returns `None` if the id isn't a 16-byte UUID.
fn coid_to_cloid(client_order_id: &str) -> Option<String> {
    let hex: String = client_order_id.chars().filter(|c| *c != '-').collect();
    if hex.len() == 32 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(format!("0x{}", hex.to_lowercase()))
    } else {
        None
    }
}

/// Round a size to `sz_decimals` decimal places.
fn round_size(sz: f64, sz_decimals: u32) -> f64 {
    let f = 10f64.powi(sz_decimals as i32);
    (sz * f).round() / f
}

/// Round a perp price to Hyperliquid's constraints: at most `6 - szDecimals`
/// decimal places, and at most 5 significant figures (integer prices are
/// always allowed). Applies the more restrictive of the two.
fn round_price(px: f64, sz_decimals: u32) -> f64 {
    if px <= 0.0 || !px.is_finite() {
        return px;
    }
    let max_decimals = 6i32.saturating_sub(sz_decimals as i32).max(0);
    // 5 significant figures
    let magnitude = px.abs().log10().floor() as i32;
    let sig_decimals = (4 - magnitude).max(0);
    let decimals = max_decimals.min(sig_decimals);
    let f = 10f64.powi(decimals);
    let r = (px * f).round() / f;
    debug!("[Hyperliquid] round_price {} -> {} (decimals={})", px, r, decimals);
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloid_from_uuid() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            coid_to_cloid(uuid).unwrap(),
            "0x550e8400e29b41d4a716446655440000"
        );
        assert!(coid_to_cloid("not-a-uuid").is_none());
    }

    #[test]
    fn price_rounding() {
        // BTC-like: szDecimals=5 → max 1 decimal, 5 sig figs
        assert_eq!(round_price(113377.37, 5), 113377.0); // 5 sig figs dominates
        assert_eq!(round_price(3650.256, 5), 3650.3); // 1 decimal
        // size rounding
        assert_eq!(round_size(0.123456, 4), 0.1235);
    }

    #[test]
    fn tif_mapping() {
        assert_eq!(tif_for(OrderType::LimitMaker, false), "Alo");
        assert_eq!(tif_for(OrderType::Limit, true), "Alo");
        assert_eq!(tif_for(OrderType::Limit, false), "Gtc");
        assert_eq!(tif_for(OrderType::Fak, false), "Ioc");
    }
}
