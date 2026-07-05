//! Aster order execution — the `ExchangeTrade` impl.
//!
//! Orders are placed/cancelled via signed V3 requests (params + EIP-712
//! signature in the query string, empty body — see [`super::signer`]). The
//! strategy's `client_order_id` (a UUID, 36 chars) is passed through as
//! `newClientOrderId` verbatim — it fits Aster's
//! `^[\.A-Z\:/a-z0-9_-]{1,36}$` rule exactly — so cancels go through
//! `origClientOrderId` without needing the exchange `orderId`.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use log::{debug, warn};
use serde::Deserialize;

use crate::types::{
    Exchange, OrderRequest, OrderStatus, OrderType, OrderUpdate, Side,
};

use super::auth::AsterAuth;
use super::info::{http_request, AsterMeta};
use super::signer::signed_query;

pub struct AsterTrade {
    auth: AsterAuth,
    meta: AsterMeta,
    /// Reserved for multi-instance tagging (single-account in v1).
    #[allow(dead_code)]
    instance_id: String,
    /// client_order_id → symbol (populated on submit; used by cancels — the
    /// V3 cancel endpoint requires the symbol alongside origClientOrderId).
    coid_symbol: HashMap<String, String>,
    /// Exchange orderIds of the orders placed by the PREVIOUS
    /// `batch_update_orders` cycle, per symbol. Cancelled (by orderId —
    /// authoritative, race-free) at the start of the next cycle. Mirrors the
    /// Hyperliquid place-then-cancel replace scheme.
    prev_oids: HashMap<String, Vec<u64>>,
}

impl AsterTrade {
    pub fn new(auth: AsterAuth, meta: AsterMeta, instance_id: &str) -> Self {
        Self {
            auth,
            meta,
            instance_id: instance_id.to_string(),
            coid_symbol: HashMap::new(),
            prev_oids: HashMap::new(),
        }
    }

    /// Sign `params` and send `method /fapi/v3/endpoint?params&signature=…`.
    /// Returns the raw body text on 2xx; the caller parses. Aster rejections
    /// come back as non-2xx with `{"code":-XXXX,"msg":"…"}` — surfaced in
    /// the error string.
    fn send_signed(&self, method: &str, endpoint: &str, params: Vec<(&str, String)>) -> Result<String> {
        let query = signed_query(&self.auth, params)?;
        let url = format!("{}?{}", self.auth.fapi_url(endpoint), query);
        http_request(method, &url)
    }

    /// Format a price for `symbol`: snap to tickSize, then render with the
    /// symbol's pricePrecision decimals, trailing zeros trimmed.
    fn fmt_price(&self, symbol: &str, px: f64) -> String {
        let (tick, prec) = self
            .meta
            .rules(symbol)
            .map(|r| (r.tick_size, r.price_precision))
            .unwrap_or((0.0, 2));
        let snapped = if tick > 0.0 { (px / tick).round() * tick } else { px };
        trim_decimal(&format!("{:.*}", prec as usize, snapped))
    }

    /// Format a quantity: snap DOWN to stepSize (never round up size), then
    /// render with quantityPrecision decimals.
    fn fmt_qty(&self, symbol: &str, qty: f64) -> String {
        let (step, prec) = self
            .meta
            .rules(symbol)
            .map(|r| (r.step_size, r.quantity_precision))
            .unwrap_or((0.0, 3));
        let snapped = if step > 0.0 { (qty / step).floor() * step } else { qty };
        trim_decimal(&format!("{:.*}", prec as usize, snapped))
    }
}

impl super::super::ExchangeTrade for AsterTrade {
    fn submit_order(&mut self, order: &OrderRequest) -> Result<OrderUpdate> {
        let symbol = order.symbol.clone();
        let is_market = order.order_type == OrderType::Market;

        let mut params: Vec<(&str, String)> = vec![
            ("symbol", symbol.clone()),
            ("side", order.side.to_string()),
        ];
        if is_market {
            params.push(("type", "MARKET".to_string()));
        } else {
            let price = order
                .price
                .ok_or_else(|| anyhow!("aster: limit order without price"))?;
            params.push(("type", "LIMIT".to_string()));
            params.push(("timeInForce", tif_for(order.order_type, order.post_only)));
            params.push(("price", self.fmt_price(&symbol, price)));
        }
        params.push(("quantity", self.fmt_qty(&symbol, order.quantity)));
        params.push(("newClientOrderId", order.client_order_id.clone()));
        // RESULT: MARKET returns the final FILLED state, LIMIT-with-special-
        // TIF the final status — saves a round-trip vs the default ACK.
        params.push(("newOrderRespType", "RESULT".to_string()));

        self.coid_symbol.insert(order.client_order_id.clone(), symbol);

        match self.send_signed("POST", "order", params) {
            Ok(text) => {
                let resp: OrderResponse = serde_json::from_str(&text)
                    .map_err(|e| anyhow!("parse /order response: {} — body: {}", e, text))?;
                Ok(resp.into_order_update(order))
            }
            // Order-level rejections (filters, post-only cross, balance…)
            // come back as HTTP 4xx `{"code":…,"msg":…}` — map to a Rejected
            // update instead of a transport error so the strategy sees a
            // normal reject, mirroring the HL error-status path.
            Err(e) => match extract_api_error(&e.to_string()) {
                Some(msg) => Ok(reject_update(order, msg)),
                None => Err(e),
            },
        }
    }

    fn cancel_order(&mut self, _exchange: Exchange, client_order_id: &str) -> Result<OrderUpdate> {
        // Unknown coid = already gone / never rested → benign no-op, like the
        // HL adapter (a stale coid must not fail a whole replace batch).
        let symbol = match self.coid_symbol.get(client_order_id).cloned() {
            Some(s) => s,
            None => {
                debug!("[Aster] cancel for unknown coid {} — no-op", client_order_id);
                return Ok(cancel_update(client_order_id, true, None));
            }
        };
        let params: Vec<(&str, String)> = vec![
            ("symbol", symbol),
            ("origClientOrderId", client_order_id.to_string()),
        ];
        let result = self.send_signed("DELETE", "order", params);
        self.coid_symbol.remove(client_order_id);
        match result {
            Ok(_) => Ok(cancel_update(client_order_id, true, None)),
            Err(e) => {
                let text = e.to_string();
                // -2011 "Unknown order sent." = already cancelled/filled.
                if text.contains("-2011") {
                    Ok(cancel_update(client_order_id, true, None))
                } else {
                    Ok(cancel_update(client_order_id, false, extract_api_error(&text)))
                }
            }
        }
    }

    /// Race-free replace: place the new quotes first (capturing their
    /// orderIds), then cancel the PREVIOUS cycle's orderIds in one batch
    /// (authoritative — a concrete orderId can't race a not-yet-placed order
    /// or silently no-op). The `cancel_client_order_ids` list from the
    /// strategy is intentionally ignored in favour of this scheme, exactly
    /// like the Hyperliquid adapter. Place-then-cancel briefly doubles the
    /// resting set (~one RTT) but never leaves a naked book.
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

        // 1. place the new quotes, collect their resting orderIds.
        let mut updates = Vec::with_capacity(place_orders.len());
        let mut new_oids = Vec::new();
        for o in place_orders {
            let u = self.submit_order(o)?;
            if u.status == OrderStatus::Accepted || u.status == OrderStatus::PartiallyFilled {
                if let Some(oid) = u.exchange_order_id.as_ref().and_then(|s| s.parse::<u64>().ok()) {
                    new_oids.push(oid);
                }
            }
            updates.push(u);
        }

        // 2. cancel the previous cycle's orderIds (best-effort, batches of 10).
        let prev = self.prev_oids.remove(&symbol).unwrap_or_default();
        for chunk in prev.chunks(10) {
            let id_list = format!(
                "[{}]",
                chunk.iter().map(|o| o.to_string()).collect::<Vec<_>>().join(",")
            );
            let params: Vec<(&str, String)> = vec![
                ("symbol", symbol.clone()),
                ("orderIdList", id_list),
            ];
            if let Err(e) = self.send_signed("DELETE", "batchOrders", params) {
                warn!("[Aster] replace cancel of {} prev oids failed: {}", chunk.len(), e);
            }
        }

        // 3. remember this cycle's oids for the next replace.
        if !new_oids.is_empty() {
            self.prev_oids.insert(symbol, new_oids);
        }
        Ok(updates)
    }

    fn cancel_all(&mut self, _exchange: Exchange, symbol: &str) -> Result<Vec<OrderUpdate>> {
        self.prev_oids.remove(symbol); // authoritative sweep supersedes tracked oids
        // Snapshot open orders first so we can ack their coids after the sweep.
        let open: Vec<OpenOrder> = match self
            .send_signed("GET", "openOrders", vec![("symbol", symbol.to_string())])
            .and_then(|t| serde_json::from_str(&t).map_err(|e| anyhow!("parse openOrders: {}", e)))
        {
            Ok(v) => v,
            Err(e) => {
                warn!("[Aster] openOrders({}) query failed: {}", symbol, e);
                Vec::new()
            }
        };
        let params: Vec<(&str, String)> = vec![("symbol", symbol.to_string())];
        let resp = self.send_signed("DELETE", "allOpenOrders", params);
        let ok = resp.is_ok();
        if let Err(e) = resp {
            warn!("[Aster] cancel_all({}) failed: {}", symbol, e);
        }
        Ok(open
            .into_iter()
            .map(|o| cancel_update(&o.client_order_id, ok, None))
            .collect())
    }

    fn name(&self) -> &str {
        "aster"
    }
}

// ════════════════════════════════════════════════════════════════
// Response parsing
// ════════════════════════════════════════════════════════════════

/// `POST /fapi/v3/order` (and cancel) response — Binance-futures shaped.
#[derive(Debug, Deserialize)]
struct OrderResponse {
    #[serde(default, rename = "orderId")]
    order_id: Option<u64>,
    #[serde(default)]
    status: String,
    #[serde(default, rename = "executedQty")]
    executed_qty: String,
    #[serde(default, rename = "origQty")]
    orig_qty: String,
    #[serde(default, rename = "avgPrice")]
    avg_price: String,
}

#[derive(Debug, Deserialize)]
struct OpenOrder {
    #[serde(default, rename = "clientOrderId")]
    client_order_id: String,
}

impl OrderResponse {
    fn into_order_update(self, order: &OrderRequest) -> OrderUpdate {
        let filled: f64 = self.executed_qty.parse().unwrap_or(0.0);
        let orig: f64 = self.orig_qty.parse().unwrap_or(order.quantity);
        let avg: f64 = self.avg_price.parse().unwrap_or(0.0);
        let (status, error) = match self.status.as_str() {
            "NEW" => (OrderStatus::Accepted, None),
            "PARTIALLY_FILLED" => (OrderStatus::PartiallyFilled, None),
            "FILLED" => (OrderStatus::Filled, None),
            "CANCELED" => (OrderStatus::Cancelled, None),
            "REJECTED" => (OrderStatus::Rejected, Some("rejected by exchange".to_string())),
            // EXPIRED on a placement = GTX would cross, or IOC/FOK remainder
            // cancelled. With fills → partial; without → effectively a reject.
            "EXPIRED" => {
                if filled > 0.0 {
                    (OrderStatus::PartiallyFilled, None)
                } else {
                    (OrderStatus::Rejected, Some("EXPIRED (post-only cross / IOC no-fill)".to_string()))
                }
            }
            other => (OrderStatus::Rejected, Some(format!("unknown status {}", other))),
        };
        OrderUpdate {
            client_order_id: order.client_order_id.clone(),
            exchange: Exchange::Aster,
            symbol: order.symbol.clone(),
            side: order.side,
            exchange_order_id: self.order_id.map(|o| o.to_string()),
            status,
            liquidity: None,
            filled_quantity: filled,
            remaining_quantity: (orig - filled).max(0.0),
            avg_fill_price: avg,
            timestamp_ns: crate::types::now_ns(),
            trade_id: None,
            error,
        }
    }
}

/// Pull the `{"code":…,"msg":"…"}` payload out of an HTTP-error string, if
/// present. Returns the whole `msg` (with code prefix) for the reject text.
fn extract_api_error(text: &str) -> Option<String> {
    let start = text.find(r#"{"code""#).or_else(|| text.find(r#"{ "code""#))?;
    let json = &text[start..];
    let end = json.find('}')? + 1;
    #[derive(Deserialize)]
    struct ApiErr {
        code: i64,
        msg: String,
    }
    serde_json::from_str::<ApiErr>(&json[..end])
        .ok()
        .map(|e| format!("{}: {}", e.code, e.msg))
}

fn reject_update(order: &OrderRequest, error: String) -> OrderUpdate {
    OrderUpdate {
        client_order_id: order.client_order_id.clone(),
        exchange: Exchange::Aster,
        symbol: order.symbol.clone(),
        side: order.side,
        exchange_order_id: None,
        status: OrderStatus::Rejected,
        liquidity: None,
        filled_quantity: 0.0,
        remaining_quantity: order.quantity,
        avg_fill_price: 0.0,
        timestamp_ns: crate::types::now_ns(),
        trade_id: None,
        error: Some(error),
    }
}

fn cancel_update(client_order_id: &str, ok: bool, err: Option<String>) -> OrderUpdate {
    OrderUpdate {
        client_order_id: client_order_id.to_string(),
        exchange: Exchange::Aster,
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
// Wire helpers
// ════════════════════════════════════════════════════════════════

/// Map an `OrderType` (+ post_only) to an Aster/Binance `timeInForce`.
/// GTX = post-only ("Good Till Crossing"), IOC = immediate-or-cancel,
/// FOK = fill-or-kill.
fn tif_for(order_type: OrderType, post_only: bool) -> String {
    match order_type {
        OrderType::LimitMaker => "GTX".to_string(),
        OrderType::Fak => "IOC".to_string(),
        OrderType::Fok => "FOK".to_string(),
        OrderType::Market => "IOC".to_string(), // unreachable (MARKET is native)
        OrderType::Limit => {
            if post_only {
                "GTX".to_string()
            } else {
                "GTC".to_string()
            }
        }
    }
}

/// Trim trailing zeros (and a bare trailing `.`) from a fixed-decimal string.
fn trim_decimal(s: &str) -> String {
    if s.contains('.') {
        let t = s.trim_end_matches('0').trim_end_matches('.');
        if t.is_empty() || t == "-" { "0".to_string() } else { t.to_string() }
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tif_mapping() {
        assert_eq!(tif_for(OrderType::LimitMaker, false), "GTX");
        assert_eq!(tif_for(OrderType::Limit, true), "GTX");
        assert_eq!(tif_for(OrderType::Limit, false), "GTC");
        assert_eq!(tif_for(OrderType::Fak, false), "IOC");
        assert_eq!(tif_for(OrderType::Fok, false), "FOK");
    }

    #[test]
    fn decimal_trimming() {
        assert_eq!(trim_decimal("50000.10"), "50000.1");
        assert_eq!(trim_decimal("50000.00"), "50000");
        assert_eq!(trim_decimal("0.00100"), "0.001");
        assert_eq!(trim_decimal("100"), "100");
    }

    #[test]
    fn api_error_extraction() {
        let e = r#"POST https://x -> 400 Bad Request: {"code":-2022,"msg":"ReduceOnly Order is rejected."}"#;
        assert_eq!(
            extract_api_error(e).unwrap(),
            "-2022: ReduceOnly Order is rejected."
        );
        assert!(extract_api_error("connection reset").is_none());
    }
}
