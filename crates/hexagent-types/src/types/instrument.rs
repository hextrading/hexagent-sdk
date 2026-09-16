use serde::{Deserialize, Serialize};

use super::market::Exchange;
use super::market::Side;

/// Settlement convention fixed at a trade's first fee attribution. Missing
/// persisted fields retain the historical V1 received-asset convention.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeeSettlement {
    #[default]
    LegacyV1,
    CollateralV2,
}

impl FeeSettlement {
    /// Return (collateral fee, outcome-share fee), without allocation.
    pub fn amounts(self, side: Side, collateral_fee: f64, price: f64) -> (f64, f64) {
        match (self, side) {
            (Self::LegacyV1, Side::Buy) => (
                0.0,
                if price > 0.0 {
                    collateral_fee / price
                } else {
                    0.0
                },
            ),
            _ => (collateral_fee, 0.0),
        }
    }

    pub fn accepts_fee_amounts(
        self,
        side: Side,
        is_maker: bool,
        usdc_fee: f64,
        shares_fee: f64,
        tolerance: f64,
    ) -> bool {
        if !usdc_fee.is_finite() || usdc_fee < 0.0 || !shares_fee.is_finite() || shares_fee < 0.0 {
            return false;
        }
        if is_maker {
            return usdc_fee <= tolerance && shares_fee <= tolerance;
        }
        match (self, side) {
            (Self::LegacyV1, Side::Buy) => usdc_fee <= tolerance,
            _ => shares_fee <= tolerance,
        }
    }
}

/// Explicit fee curve supplied by an adapter or token registry. The account
/// freezes the selected basis once; fallback selection is an adapter policy.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FeeBasis {
    #[serde(default)]
    pub settlement: FeeSettlement,
    pub rate: f64,
    pub exponent: f64,
}

/// The account owner's immutable fee attribution for one private trade.
/// Amounts remain the original fees on FAILED so consumers can reverse them.
/// Presence also proves real zero-fee attribution (maker or zero-rate curve).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TradeFee {
    pub settlement: FeeSettlement,
    pub usdc_fee: f64,
    pub shares_fee: f64,
}

/// A spot trading instrument (e.g. Binance BTCUSDT)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotInstrument {
    pub exchange: Exchange,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
}

/// A binary option / prediction market (e.g. Polymarket or HexMarket YES/NO market)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryOption {
    pub exchange: Exchange,
    pub id: String,
    pub question: String,
    pub condition_id: String,
    /// Stable subscription/series identity. Unlike `slug`, this does not
    /// rotate with each timed event and is therefore safe for instance routing.
    #[serde(default)]
    pub series_slug: String,
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
    /// Explicit protocol convention. Historical recordings without this field
    /// remain V1; live V2 metadata must opt into collateral fees.
    #[serde(default)]
    pub fee_settlement: FeeSettlement,
}

impl BinaryOption {
    pub fn validate_polymarket_fee_curve(
        fee_rate: f64,
        fee_exponent: f64,
        fee_rate_bps: u32,
    ) -> Result<(), String> {
        if !fee_rate.is_finite() || !(0.0..=1.0).contains(&fee_rate) {
            return Err(format!(
                "fee rate must be finite and in [0, 1], got {fee_rate}"
            ));
        }
        if !fee_exponent.is_finite() || fee_exponent <= 0.0 || fee_exponent > 10.0 {
            return Err(format!(
                "fee exponent must be finite and in (0, 10], got {fee_exponent}"
            ));
        }
        if fee_rate_bps > 10_000 {
            return Err(format!("fee rate bps must be <= 10000, got {fee_rate_bps}"));
        }
        Ok(())
    }

    pub fn validate_fee_curve(&self) -> Result<(), String> {
        Self::validate_polymarket_fee_curve(self.fee_rate, self.fee_exponent, self.base_fee)
    }

    /// Polymarket taker fee expressed in USDC, ignoring side:
    ///   `usdc_fee = C × fee_rate × (p × (1 − p)) ^ fee_exponent`
    /// where `C` is the trade size in shares and `p` is the trade price.
    /// Makers pay no fee — callers must gate on the TAKER role.
    pub fn taker_fee_usdc(&self, size: f64, price: f64) -> f64 {
        if self.validate_fee_curve().is_err()
            || !size.is_finite()
            || size <= 0.0
            || !price.is_finite()
        {
            return 0.0;
        }
        let p = price.clamp(0.0, 1.0);
        let pp = (p * (1.0 - p)).max(0.0);
        size * self.fee_rate * pp.powf(self.fee_exponent)
    }

    /// Explicit collateral/share amounts for the configured protocol version.
    pub fn taker_fee_amounts(&self, size: f64, price: f64, side: Side) -> (f64, f64) {
        self.fee_settlement
            .amounts(side, self.taker_fee_usdc(size, price), price)
    }

    /// Fee in its settlement asset (shares for V1 BUY, collateral otherwise).
    pub fn taker_fee_charged(&self, size: f64, price: f64, side: &str) -> f64 {
        let side = if side.eq_ignore_ascii_case("BUY") {
            Side::Buy
        } else if side.eq_ignore_ascii_case("SELL") {
            Side::Sell
        } else {
            return 0.0;
        };
        let (cash, shares) = self.taker_fee_amounts(size, price, side);
        cash + shares
    }
}

/// Instrument types supported by the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Instrument {
    Spot(SpotInstrument),
    BinaryOption(BinaryOption),
}

#[cfg(test)]
mod fee_settlement_tests {
    use super::*;

    #[test]
    fn v1_and_v2_fee_currencies_are_explicit_on_both_sides() {
        let fee = 14.0 * 0.07 * (0.85 * 0.15);
        let v1 = FeeSettlement::LegacyV1.amounts(Side::Buy, fee, 0.85);
        let v2 = FeeSettlement::CollateralV2.amounts(Side::Buy, fee, 0.85);
        assert_eq!(v1.0, 0.0);
        assert!((v1.1 - 0.147).abs() < 1e-12);
        assert!((v2.0 - 0.12495).abs() < 1e-12);
        assert_eq!(v2.1, 0.0);
        for version in [FeeSettlement::LegacyV1, FeeSettlement::CollateralV2] {
            assert_eq!(version.amounts(Side::Sell, fee, 0.85), (fee, 0.0));
            assert!(version.accepts_fee_amounts(Side::Sell, false, fee, 0.0, 1e-9));
            assert!(!version.accepts_fee_amounts(Side::Sell, true, fee, 0.0, 1e-9));
        }
        assert_eq!(FeeSettlement::default(), FeeSettlement::LegacyV1);
    }

    #[test]
    #[ignore = "focused local scalar benchmark; excludes HTTP, queues and strategy scheduling"]
    fn fee_settlement_scalar_benchmark() {
        use std::hint::black_box;
        use std::time::Instant;
        const N: usize = 100_000;
        for version in [FeeSettlement::LegacyV1, FeeSettlement::CollateralV2] {
            let mut samples = Vec::with_capacity(N);
            for i in 0..N + 4096 {
                let price = black_box(0.01 + (i % 98) as f64 * 0.01);
                let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
                let start = Instant::now();
                let fee = black_box(14.0)
                    * black_box(0.07)
                    * (price * (1.0 - price)).powf(black_box(1.0));
                black_box(version.amounts(black_box(side), fee, price));
                let elapsed = start.elapsed().as_nanos();
                if i >= 4096 {
                    samples.push(elapsed);
                }
            }
            samples.sort_unstable();
            eprintln!("fee_settlement boundary=curve_plus_asset_split version={version:?} n={N} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=not_applicable overflow=not_applicable", samples[(N-1)/2], samples[(N-1)*99/100], samples[(N-1)*999/1000], samples[N-1]);
        }
    }
}
