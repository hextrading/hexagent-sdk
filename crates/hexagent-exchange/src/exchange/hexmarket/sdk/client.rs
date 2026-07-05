//! Hexmarket REST client built on the shared async tokio runtime + reqwest.
//!
//! Replaces the upstream `hexmarket_sdk_sync::HexClient`, which used blocking
//! `ureq` internally — this one uses the process-wide auto-negotiating reqwest
//! client so connection reuse and TLS session caching are shared with every
//! other HTTP endpoint in the binary.

use std::sync::RwLock;

use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;

use super::auth::{build_l2_headers, ApiCredentials, L2Headers};
use super::types::*;

/// Pick a stable latency-histogram stage name for a Hexmarket REST
/// request. Buckets by (method, path prefix); extend as needed.
fn hex_http_stage(method: &str, path: &str) -> &'static str {
    if path == "/api/v1/orders" {
        match method {
            "POST" => return "hexmarket.http.place_order",
            "DELETE" => return "hexmarket.http.cancel_all",
            _ => {}
        }
    }
    if path == "/api/v1/orders/batch" {
        match method {
            "POST" => return "hexmarket.http.place_batch",
            "DELETE" => return "hexmarket.http.cancel_batch",
            "PUT" => return "hexmarket.http.update_batch",
            _ => {}
        }
    }
    if path.starts_with("/api/v1/orders/client/") {
        return "hexmarket.http.cancel_order";
    }
    if path.starts_with("/api/v1/positions") { return "hexmarket.http.get_positions"; }
    if path.starts_with("/api/v1/balances") { return "hexmarket.http.get_balance"; }
    if path.starts_with("/api/v1/orderbook/") { return "hexmarket.http.get_orderbook"; }
    if path.starts_with("/api/v1/events") { return "hexmarket.http.get_events"; }
    match method {
        "GET" => "hexmarket.http.get_other",
        "POST" => "hexmarket.http.post_other",
        "DELETE" => "hexmarket.http.delete_other",
        "PUT" => "hexmarket.http.put_other",
        _ => "hexmarket.http.other",
    }
}

/// Client configuration.
#[derive(Debug, Clone)]
pub struct HexClientConfig {
    /// API base URL, e.g. `https://apidev.hexmarket.xyz`.
    pub api_url: String,
}

/// Hexmarket REST client. Thread-safe, cheap to share via `Arc`.
pub struct HexClient {
    base_url: String,
    credentials: RwLock<Option<(String, ApiCredentials)>>,
}

impl HexClient {
    pub fn new(config: HexClientConfig) -> Self {
        Self {
            base_url: config.api_url.trim_end_matches('/').to_string(),
            credentials: RwLock::new(None),
        }
    }

    pub fn set_credentials(&self, pubkey: &str, creds: ApiCredentials) {
        *self.credentials.write().unwrap() = Some((pubkey.to_string(), creds));
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn l2_headers(&self, method: &str, path: &str, body: Option<&str>) -> Result<L2Headers> {
        let guard = self.credentials.read().unwrap();
        let (pubkey, creds) = guard.as_ref()
            .ok_or_else(|| anyhow!("Hexmarket: missing API credentials"))?;
        build_l2_headers(creds, pubkey, method, path, body)
    }

    fn require_pubkey(&self) -> Result<String> {
        let guard = self.credentials.read().unwrap();
        let (pubkey, _) = guard.as_ref()
            .ok_or_else(|| anyhow!("Hexmarket: missing API credentials"))?;
        Ok(pubkey.clone())
    }

    // ─── Shared HTTP execution ───────────────────────────────

    /// Perform a request via the shared async runtime + auto-negotiating
    /// reqwest client. Parses the response as JSON `T` on 2xx, returns a
    /// formatted `anyhow::Error` on non-2xx (embedding the server's body
    /// for diagnostics, matching the old SDK's behaviour).
    fn execute_json<T>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
        authed: bool,
    ) -> Result<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let url = self.url(path);
        let body_str = body.as_ref().map(|b| b.to_string());
        let headers: Option<[(&'static str, String); 5]> = if authed {
            let h = self.l2_headers(method.as_str(), path, body_str.as_deref())?;
            Some([
                ("HEX-ADDRESS", h.address),
                ("HEX-API-KEY", h.api_key),
                ("HEX-PASSPHRASE", h.passphrase),
                ("HEX-TIMESTAMP", h.timestamp),
                ("HEX-SIGNATURE", h.signature),
            ])
        } else {
            None
        };

        // Role by method: places → Fast, cancels → Cancel, reads → Query
        // (shared h1.1 pools).
        let client = crate::http1_pool::client(match method {
            reqwest::Method::POST => crate::http1_pool::Role::Fast,
            reqwest::Method::DELETE => crate::http1_pool::Role::Cancel,
            _ => crate::http1_pool::Role::Query,
        });
        let stage = hex_http_stage(method.as_str(), path);
        let t_start = crate::latency::Instant::now();
        let result = crate::async_rt::block_on_runtime(async move {
            let mut req = client.request(method.clone(), &url);
            if let Some(pairs) = &headers {
                for (k, v) in pairs {
                    req = req.header(*k, v.as_str());
                }
            }
            if let Some(b) = body_str {
                req = req.header("Content-Type", "application/json").body(b);
            }
            let resp = req.send().await
                .map_err(|e| anyhow!("{} {} failed: {}", method, url, e))?;
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            if !status.is_success() {
                return Err(anyhow!("{} {} failed ({}): {}", method, url, status, text));
            }
            serde_json::from_str::<T>(&text)
                .map_err(|e| anyhow!("parse {} {}: {} (body={})", method, url, e, text))
        });
        crate::latency::record(stage, t_start);
        result
    }

    fn get<T: DeserializeOwned + Send + 'static>(&self, path: &str) -> Result<T> {
        self.execute_json(reqwest::Method::GET, path, None, false)
    }

    fn get_auth<T: DeserializeOwned + Send + 'static>(&self, path: &str) -> Result<T> {
        self.execute_json(reqwest::Method::GET, path, None, true)
    }

    fn post_auth<T: DeserializeOwned + Send + 'static>(
        &self, path: &str, body: serde_json::Value,
    ) -> Result<T> {
        self.execute_json(reqwest::Method::POST, path, Some(body), true)
    }

    fn put_auth<T: DeserializeOwned + Send + 'static>(
        &self, path: &str, body: serde_json::Value,
    ) -> Result<T> {
        self.execute_json(reqwest::Method::PUT, path, Some(body), true)
    }

    fn delete_auth<T: DeserializeOwned + Send + 'static>(&self, path: &str) -> Result<T> {
        self.execute_json(reqwest::Method::DELETE, path, None, true)
    }

    fn delete_auth_with_body<T: DeserializeOwned + Send + 'static>(
        &self, path: &str, body: serde_json::Value,
    ) -> Result<T> {
        self.execute_json(reqwest::Method::DELETE, path, Some(body), true)
    }

    // ─── Public endpoints ─────────────────────────────────────

    pub fn list_events(&self, params: &ListEventsParams) -> Result<Vec<EventListItem>> {
        let mut path = String::from("/api/v1/events");
        let mut sep = '?';
        if let Some(ref t) = params.tag {
            path.push_str(&format!("{}tag={}", sep, t)); sep = '&';
        }
        if let Some(ref s) = params.status {
            path.push_str(&format!("{}status={}", sep, s)); sep = '&';
        }
        if let Some(l) = params.limit {
            path.push_str(&format!("{}limit={}", sep, l)); sep = '&';
        }
        if let Some(o) = params.offset {
            path.push_str(&format!("{}offset={}", sep, o));
            let _ = sep;
        }
        self.get(&path)
    }

    pub fn get_event(&self, slug: &str) -> Result<EventDetail> {
        self.get(&format!("/api/v1/events/{}", slug))
    }

    pub fn get_orderbook(&self, outcome_id: &str) -> Result<OrderBook> {
        self.get(&format!("/api/v1/orderbook/{}", outcome_id))
    }

    // ─── Authenticated endpoints ──────────────────────────────

    pub fn get_balance(&self) -> Result<UserBalance> {
        let pubkey = self.require_pubkey()?;
        self.get_auth(&format!("/api/v1/balances?user={}", pubkey))
    }

    pub fn get_positions(&self) -> Result<Vec<Position>> {
        let pubkey = self.require_pubkey()?;
        self.get_auth(&format!("/api/v1/positions?user={}", pubkey))
    }

    pub fn get_open_orders(&self, outcome_id: Option<&str>) -> Result<Vec<Order>> {
        let pubkey = self.require_pubkey()?;
        let mut path = format!("/api/v1/orders?user={}&status=open", pubkey);
        if let Some(oid) = outcome_id {
            path.push_str(&format!("&outcome_id={}", oid));
        }
        self.get_auth(&path)
    }

    pub fn place_order(&self, params: &PlaceOrderParams) -> Result<PlaceOrderResponse> {
        let body = serde_json::to_value(params)
            .map_err(|e| anyhow!("serialize PlaceOrderParams: {}", e))?;
        self.post_auth("/api/v1/orders", body)
    }

    pub fn cancel_order_by_client_id(&self, client_order_id: &str) -> Result<CancelOrderResponse> {
        self.delete_auth(&format!("/api/v1/orders/client/{}", client_order_id))
    }

    pub fn cancel_all_orders(
        &self,
        market_id: Option<&str>,
        event_id: Option<&str>,
    ) -> Result<CancelAllOrdersResponse> {
        let mut path = String::from("/api/v1/orders");
        let mut parts = Vec::new();
        if let Some(mid) = market_id { parts.push(format!("market_id={}", mid)); }
        if let Some(eid) = event_id { parts.push(format!("event_id={}", eid)); }
        if !parts.is_empty() { path.push('?'); path.push_str(&parts.join("&")); }
        self.delete_auth(&path)
    }

    pub fn batch_place_orders(
        &self,
        market_id: &str,
        orders: &[PlaceOrderParams],
    ) -> Result<BatchPlaceResponse> {
        let body = serde_json::json!({
            "market_id": market_id,
            "orders": orders,
        });
        self.post_auth("/api/v1/orders/batch", body)
    }

    pub fn batch_cancel_orders(
        &self,
        market_id: &str,
        order_ids: &[&str],
        client_order_ids: &[&str],
    ) -> Result<BatchCancelResponse> {
        let body = serde_json::json!({
            "market_id": market_id,
            "order_ids": order_ids,
            "client_order_ids": client_order_ids,
        });
        self.delete_auth_with_body("/api/v1/orders/batch", body)
    }

    pub fn batch_update_orders(
        &self,
        market_id: &str,
        cancel_order_ids: &[&str],
        place_orders: &[PlaceOrderParams],
        cancel_client_order_ids: Option<&[&str]>,
    ) -> Result<BatchUpdateResponse> {
        let mut body = serde_json::json!({
            "market_id": market_id,
            "cancel_order_ids": cancel_order_ids,
            "place_orders": place_orders,
        });
        if let Some(ids) = cancel_client_order_ids {
            body["cancel_client_order_ids"] = serde_json::json!(ids);
        }
        self.put_auth("/api/v1/orders/batch", body)
    }
}
