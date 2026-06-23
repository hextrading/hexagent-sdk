//! Domain types for the Hexmarket API — mirrors the server's JSON schema.
//!
//! Only types actually consumed by hexbot are defined here; the upstream
//! `hexmarket_sdk_sync` crate defines more but we don't use them.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ────────────────────────────────────────────────────────────
// Order enums
// ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Buy => write!(f, "buy"),
            Side::Sell => write!(f, "sell"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    Market,
}

impl std::fmt::Display for OrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderType::Limit => write!(f, "limit"),
            OrderType::Market => write!(f, "market"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Gtc,
    Ioc,
    Fok,
}

impl std::fmt::Display for TimeInForce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeInForce::Gtc => write!(f, "gtc"),
            TimeInForce::Ioc => write!(f, "ioc"),
            TimeInForce::Fok => write!(f, "fok"),
        }
    }
}

// ────────────────────────────────────────────────────────────
// Order request + responses
// ────────────────────────────────────────────────────────────

/// Request body for `POST /api/v1/orders` (and batch variants).
#[derive(Debug, Clone, Serialize)]
pub struct PlaceOrderParams {
    pub outcome_id: String,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub price: Decimal,
    pub quantity: u64,
    pub nonce: u64,
    pub signature: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_pubkey: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct PlaceOrderResponse {
    pub order_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct CancelOrderResponse {
    pub order_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct CancelAllOrdersResponse {
    pub cancelled_count: usize,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub orders: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct BatchPlaceResult {
    #[serde(default)]
    pub index: usize,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchPlaceResponse {
    pub results: Vec<BatchPlaceResult>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct BatchCancelResult {
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchCancelResponse {
    pub results: Vec<BatchCancelResult>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BatchUpdateResponse {
    pub cancel_results: Vec<BatchCancelResult>,
    pub place_results: Vec<BatchPlaceResult>,
}

// ────────────────────────────────────────────────────────────
// User state (open orders, positions, balance)
// ────────────────────────────────────────────────────────────

/// Open order record (subset of server's full Order schema).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Order {
    pub id: uuid::Uuid,
    pub outcome_id: uuid::Uuid,
    pub user_pubkey: String,
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub price: Decimal,
    pub quantity: i64,
    #[serde(default)]
    pub filled_quantity: i64,
    pub remaining_quantity: i64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Position {
    pub user_pubkey: String,
    pub outcome_id: uuid::Uuid,
    pub quantity: i64,
    #[serde(default)]
    pub avg_price: Option<Decimal>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct UserBalance {
    pub user_pubkey: String,
    pub usdc_balance: i64,
    #[serde(default)]
    pub locked_usdc: i64,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

// ────────────────────────────────────────────────────────────
// Orderbook
// ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookLevel {
    pub price: Decimal,
    pub quantity: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct OrderBook {
    pub outcome_id: uuid::Uuid,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
}

// ────────────────────────────────────────────────────────────
// Events / markets / outcomes
// ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Outcome {
    pub id: uuid::Uuid,
    pub market_id: uuid::Uuid,
    pub label: String,
    #[serde(default)]
    pub label_translations: Option<HashMap<String, String>>,
    #[serde(default)]
    pub sort_order: i32,
    #[serde(default)]
    pub outcome_index: i32,
    #[serde(default)]
    pub price: Option<Decimal>,
    #[serde(default)]
    pub liquidity: Option<Decimal>,
    #[serde(default)]
    pub total_volume: Option<Decimal>,
    // Remaining server fields are ignored to keep the struct small; serde
    // will accept unknown fields since we don't use deny_unknown_fields.
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Market {
    pub id: uuid::Uuid,
    pub event_id: uuid::Uuid,
    pub title: String,
    #[serde(default)]
    pub market_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub price_increment: Option<Decimal>,
    // Other server-side fields intentionally omitted.
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct HexEvent {
    pub id: uuid::Uuid,
    pub slug: String,
    pub title: String,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketDetail {
    #[serde(flatten)]
    pub market: Market,
    pub outcomes: Vec<Outcome>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventListItem {
    #[serde(flatten)]
    pub event: HexEvent,
    pub markets: Vec<MarketDetail>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventDetail {
    #[serde(flatten)]
    pub event: HexEvent,
    pub markets: Vec<MarketDetail>,
}

// ────────────────────────────────────────────────────────────
// Query parameters for public endpoints
// ────────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct ListEventsParams {
    pub tag: Option<String>,
    pub status: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}
