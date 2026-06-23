use serde::{Deserialize, Serialize};

use super::market::Exchange;

/// A spot trading instrument (e.g. Binance BTCUSDT)
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct SpotInstrument {
    pub exchange: Exchange,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
}

/// A binary option / prediction market (e.g. Polymarket or HexMarket YES/NO market)
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct BinaryOption {
    pub exchange: Exchange,
    pub id: String,
    pub question: String,
    pub condition_id: String,
    pub slug: String,
    pub clob_token_ids: Vec<String>,
    pub outcomes: Vec<String>,
    pub outcome_prices: Vec<String>,
    pub active: bool,
    pub closed: bool,
    pub volume: f64,
    pub liquidity: f64,
    pub tick_size: f64,
    pub order_min_size: f64,
    /// Group item title for categorical markets (e.g. "Anthropic", "OpenAI").
    /// Used for cross-exchange matching: hex market title ↔ poly group_item_title.
    #[serde(default)]
    pub group_item_title: String,
    /// Event start time (ISO 8601, e.g. "2026-03-29T06:10:00Z").
    #[serde(default)]
    pub event_start_time: String,
    /// Taker base fee in basis points. Sourced from the event API's `takerBaseFee`
    /// field (e.g. Polymarket Gamma event → market.takerBaseFee).
    #[serde(default)]
    pub base_fee: u32,
    /// Fee curve exponent, from event API's `feeSchedule.exponent`.
    #[serde(default)]
    pub fee_exponent: f64,
    /// Fee rate, from event API's `feeSchedule.rate`.
    #[serde(default)]
    pub fee_rate: f64,
}

impl BinaryOption {
    /// Polymarket taker fee expressed in USDC, ignoring side:
    ///   `usdc_fee = C × fee_rate × (p × (1 − p)) ^ fee_exponent`
    /// where `C` is the trade size in shares and `p` is the trade price.
    /// Makers pay no fee — callers must gate on the TAKER role.
    pub fn taker_fee_usdc(&self, size: f64, price: f64) -> f64 {
        if self.fee_rate <= 0.0 || size <= 0.0 {
            return 0.0;
        }
        let p = price.clamp(0.0, 1.0);
        let pp = (p * (1.0 - p)).max(0.0);
        size * self.fee_rate * pp.powf(self.fee_exponent)
    }

    /// Actual amount deducted from the taker for a fill, in the currency the
    /// fee is paid in:
    /// - BUY  → fee is taken out of the purchased shares. Returned value is in
    ///   shares: `usdc_fee / p`.
    /// - SELL → fee is taken out of the USDC proceeds. Returned value is in
    ///   USDC: `usdc_fee`.
    /// Returns 0 for any non-BUY/SELL side or if `price` is non-positive on BUY.
    pub fn taker_fee_charged(&self, size: f64, price: f64, side: &str) -> f64 {
        let usdc = self.taker_fee_usdc(size, price);
        match side.to_ascii_uppercase().as_str() {
            "BUY" => if price > 0.0 { usdc / price } else { 0.0 },
            "SELL" => usdc,
            _ => 0.0,
        }
    }
}

/// Instrument types supported by the system
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub enum Instrument {
    Spot(SpotInstrument),
    BinaryOption(BinaryOption),
}
