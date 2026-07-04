use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported exchanges
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    Binance,
    Bybit,
    Coinbase,
    Kraken,
    Okx,
    Gate,
    Bitget,
    Kucoin,
    Mexc,
    Polymarket,
    Hexmarket,
    Hyperliquid,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Binance => write!(f, "binance"),
            Exchange::Bybit => write!(f, "bybit"),
            Exchange::Coinbase => write!(f, "coinbase"),
            Exchange::Kraken => write!(f, "kraken"),
            Exchange::Okx => write!(f, "okx"),
            Exchange::Gate => write!(f, "gate"),
            Exchange::Bitget => write!(f, "bitget"),
            Exchange::Kucoin => write!(f, "kucoin"),
            Exchange::Mexc => write!(f, "mexc"),
            Exchange::Polymarket => write!(f, "polymarket"),
            Exchange::Hexmarket => write!(f, "hexmarket"),
            Exchange::Hyperliquid => write!(f, "hyperliquid"),
        }
    }
}

impl Exchange {
    pub fn from_name(name: &str) -> Option<Exchange> {
        match name {
            "binance" => Some(Exchange::Binance),
            "bybit" => Some(Exchange::Bybit),
            "coinbase" => Some(Exchange::Coinbase),
            "kraken" => Some(Exchange::Kraken),
            "okx" => Some(Exchange::Okx),
            "gate" => Some(Exchange::Gate),
            "bitget" => Some(Exchange::Bitget),
            "kucoin" => Some(Exchange::Kucoin),
            "mexc" => Some(Exchange::Mexc),
            "polymarket" => Some(Exchange::Polymarket),
            "hexmarket" => Some(Exchange::Hexmarket),
            "hyperliquid" => Some(Exchange::Hyperliquid),
            _ => None,
        }
    }
}

/// Trade side
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Side::Buy => write!(f, "BUY"),
            Side::Sell => write!(f, "SELL"),
        }
    }
}

/// A single price level in the order book
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct PriceLevel {
    pub price: f64,
    pub quantity: f64,
}

/// Full order book snapshot
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct OrderBookSnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub exchange_timestamp_ns: u64,
    pub local_timestamp_ns: u64,
}

/// A single trade event
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct TradeTick {
    pub exchange: Exchange,
    pub symbol: String,
    pub price: f64,
    pub quantity: f64,
    pub side: Side,
    pub exchange_timestamp_ns: u64,
    pub local_timestamp_ns: u64,
}

/// Best bid/ask quote tick (from bookTicker stream)
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct QuoteTick {
    pub exchange: Exchange,
    pub symbol: String,
    pub bid_price: f64,
    pub bid_qty: f64,
    pub ask_price: f64,
    pub ask_qty: f64,
    pub exchange_timestamp_ns: u64,
    pub local_timestamp_ns: u64,
}

/// OHLCV kline/candlestick bar
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct BarData {
    pub exchange: Exchange,
    pub symbol: String,
    pub interval: String,
    pub open_time_ns: u64,
    pub close_time_ns: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    /// Taker (aggressive) BUY base volume — Binance kline `taker_buy_base`
    /// (REST `[9]`, ws `V`). `sell_volume = volume − taker_buy_base`.
    /// `0.0` when the source omits it (e.g. legacy 1m parquet). Feeds the
    /// optional HAR residual-model order-flow |OFI| feature.
    #[serde(default)]
    pub taker_buy_base: f64,
    /// Quote-asset volume (Σ price·qty) — Binance kline `quote_volume`
    /// (REST `[7]`, ws `q`). Bar VWAP = `quote_volume / volume`. `0.0`
    /// when omitted. Feeds the residual-model |VWAP−close| feature.
    #[serde(default)]
    pub quote_volume: f64,
    pub is_closed: bool,
    pub exchange_timestamp_ns: u64,
    pub local_timestamp_ns: u64,
}

/// Tick size change notification (Polymarket)
#[derive(
    Debug, Clone, Serialize, Deserialize,
)]
pub struct TickSizeChange {
    pub exchange: Exchange,
    pub symbol: String,
    pub old_tick_size: f64,
    pub new_tick_size: f64,
    pub local_timestamp_ns: u64,
}

/// Real-time spot price from external data source (e.g. Polymarket RTDS).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotPrice {
    /// Data source: "chainlink", "pyth", or legacy "rtds_binance", etc.
    pub source: String,
    pub symbol: String,
    pub price: f64,
    /// Server-side timestamp (nanoseconds).
    pub timestamp_ns: u64,
    pub local_timestamp_ns: u64,
}

/// Request for historical kline data, returned by `Strategy::load_hist_data()`.
#[derive(Debug, Clone)]
pub struct HistDataRequest {
    pub exchange: Exchange,
    pub symbol: String,
    /// Kline interval: "1m", "5m", "15m", "1h", "1d", etc.
    pub interval: String,
    /// Start timestamp (inclusive), nanoseconds since epoch.
    pub start_date_ns: u64,
    /// End timestamp (exclusive), nanoseconds since epoch.
    pub end_date_ns: u64,
}

impl OrderBookSnapshot {
    /// True best bid: the level with the **highest price** in the bids
    /// array. Independent of the exchange's chosen array ordering.
    ///
    /// Background: different exchanges populate `bids` in opposite
    /// directions. Polymarket sends bids ascending (low → high, best
    /// at the end); Binance / Coinbase / OKX / Bybit / etc. send bids
    /// descending (high → low, best at the start). The previous
    /// implementation used `bids.last()` unconditionally — correct for
    /// Polymarket but returning the WORST-in-depth level for every
    /// non-Polymarket exchange. Combined with the recorder's
    /// truncate-to-top-5 step, mid prices for spot exchanges were
    /// silently off by $3–$5 per tick (the depth between best and the
    /// 5th level).
    ///
    /// This implementation iterates and picks the max price, so it's
    /// correct regardless of the upstream's ordering convention. The
    /// cost is O(N) per call where N ≤ 20 levels, negligible vs even
    /// a single arithmetic op on the spot price.
    ///
    /// Filters out non-finite (NaN / Inf) and non-positive prices, so a
    /// corrupt parser output can't leak into downstream math.
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.iter()
            .filter(|l| l.price.is_finite() && l.price > 0.0)
            .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal))
    }

    /// True best ask: the level with the **lowest price** in the asks
    /// array. Mirror of `best_bid` — see that fn's doc for why this
    /// is order-independent. Polymarket sends asks descending (best
    /// at end), spot exchanges send asks ascending (best at start);
    /// both work with this implementation.
    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.iter()
            .filter(|l| l.price.is_finite() && l.price > 0.0)
            .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal))
    }

    pub fn mid_price(&self) -> f64 {
        let best_bid_price = if let Some(bid_level) = self.best_bid() {
            bid_level.price
        } else {
            0.0
        };
        let best_ask_price = if let Some(ask_level) = self.best_ask() {
            ask_level.price
        } else {
            1.0
        };
        (best_bid_price + best_ask_price)/2.0
        // (self.best_bid().un(0.0) + self.best_ask().unwrap_or(1.0))/2.0
        // match (self.best_bid(), self.best_ask()) {
        //     (Some(bid), Some(ask)) => Some((bid.price + ask.price) / 2.0),
        //     _ => None,
        // }
    }

    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.price - bid.price),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    //! Tests for `best_bid` / `best_ask` correctness across exchange
    //! ordering conventions.
    //!
    //! Background — pre-fix the methods returned `bids.last()` and
    //! `asks.last()` unconditionally. That's correct for Polymarket
    //! (bids ascending, asks descending — best at end) but returns the
    //! WORST-in-depth level for every spot exchange (Binance / Coinbase
    //! / OKX / Bybit / Kucoin / Gate / MEXC / Bitget), which all send
    //! bids descending and asks ascending — best at the start. The
    //! tests below lock the new "order-independent max/min" semantics:
    //! whichever direction the upstream sorts, `best_bid` returns the
    //! highest-price level and `best_ask` returns the lowest-price level.
    use super::*;

    fn lvl(price: f64, qty: f64) -> PriceLevel {
        PriceLevel { price, quantity: qty }
    }

    fn book(exchange: Exchange, bids: Vec<PriceLevel>, asks: Vec<PriceLevel>) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange,
            symbol: "TEST".to_string(),
            bids,
            asks,
            exchange_timestamp_ns: 0,
            local_timestamp_ns: 0,
        }
    }

    /// Polymarket convention: bids ascending, asks descending. The pre-fix
    /// behaviour got this case right by accident (`last()` was always the
    /// best). The new implementation must still get it right.
    #[test]
    fn polymarket_ordering_returns_correct_best() {
        let ob = book(
            Exchange::Polymarket,
            vec![lvl(0.40, 10.0), lvl(0.45, 20.0), lvl(0.50, 5.0)], // ascending
            vec![lvl(0.60, 10.0), lvl(0.55, 20.0), lvl(0.51, 5.0)], // descending
        );
        assert_eq!(ob.best_bid().unwrap().price, 0.50, "highest bid (last in ascending)");
        assert_eq!(ob.best_ask().unwrap().price, 0.51, "lowest ask (last in descending)");
    }

    /// Binance / Coinbase / etc. convention: bids descending, asks
    /// ascending. The pre-fix `bids.last()` returned the WORST bid (lowest
    /// in a descending list) and `asks.last()` returned the WORST ask
    /// (highest in an ascending list). The new implementation must
    /// return the highest bid / lowest ask regardless.
    ///
    /// Regression guard for the "BTCUSDT mid off by $3–$5 per tick" bug
    /// described in commits 7c195e8 / bf02040.
    #[test]
    fn binance_descending_bids_returns_highest_not_last() {
        // Real Binance @depth10 sample from 2026-05-13 (5 levels each side).
        let ob = book(
            Exchange::Binance,
            vec![
                lvl(79624.06, 0.3342),   // <- HIGHEST = true best bid
                lvl(79624.05, 0.0007),
                lvl(79624.00, 0.0014),
                lvl(79622.72, 0.0044),
                lvl(79622.33, 0.0001),   // <- pre-fix returned THIS (wrong)
            ],
            vec![
                lvl(79624.07, 5.0076),   // <- LOWEST = true best ask
                lvl(79624.08, 0.0022),
                lvl(79624.74, 0.0001),
                lvl(79626.00, 0.3011),
                lvl(79626.01, 0.3423),   // <- pre-fix returned THIS (wrong)
            ],
        );
        assert_eq!(ob.best_bid().unwrap().price, 79624.06,
            "Binance descending bids: best is bids[0]=79624.06, NOT bids[-1]=79622.33");
        assert_eq!(ob.best_ask().unwrap().price, 79624.07,
            "Binance ascending asks: best is asks[0]=79624.07, NOT asks[-1]=79626.01");
    }

    /// Defensive: even a fully-shuffled input must still produce the
    /// correct best. Real exchanges always sort, but the order-
    /// independent implementation also defends against future parsers
    /// or DIY OB construction code that might not.
    #[test]
    fn unordered_input_still_finds_correct_best() {
        let ob = book(
            Exchange::Binance,
            // Deliberately scrambled
            vec![lvl(79622.33, 1.0), lvl(79624.06, 1.0), lvl(79623.00, 1.0), lvl(79622.72, 1.0)],
            vec![lvl(79626.00, 1.0), lvl(79624.74, 1.0), lvl(79624.07, 1.0), lvl(79626.01, 1.0)],
        );
        assert_eq!(ob.best_bid().unwrap().price, 79624.06);
        assert_eq!(ob.best_ask().unwrap().price, 79624.07);
    }

    /// Empty and single-element edge cases.
    #[test]
    fn empty_returns_none_single_returns_that_element() {
        let empty = book(Exchange::Binance, vec![], vec![]);
        assert!(empty.best_bid().is_none());
        assert!(empty.best_ask().is_none());

        let single = book(
            Exchange::Binance,
            vec![lvl(79624.06, 0.5)],
            vec![lvl(79624.07, 0.5)],
        );
        assert_eq!(single.best_bid().unwrap().price, 79624.06);
        assert_eq!(single.best_ask().unwrap().price, 79624.07);
    }

    /// Non-finite (NaN / Inf) and non-positive prices are filtered out.
    /// Real parsers handle this upstream but the methods defend in case
    /// a corrupt input slips through (e.g. a JSON with NaN literal,
    /// or an empty-string price defaulting to 0.0).
    #[test]
    fn non_finite_and_non_positive_levels_are_filtered() {
        let ob = book(
            Exchange::Binance,
            vec![
                lvl(f64::NAN, 1.0),
                lvl(0.0, 1.0),           // zero filtered
                lvl(-100.0, 1.0),        // negative filtered
                lvl(79624.06, 1.0),      // valid
                lvl(79623.00, 1.0),
                lvl(f64::INFINITY, 1.0), // infinity filtered (would otherwise be "max")
            ],
            vec![
                lvl(79624.07, 1.0),      // valid lowest
                lvl(f64::NAN, 1.0),
                lvl(0.0, 1.0),
                lvl(79625.00, 1.0),
            ],
        );
        assert_eq!(ob.best_bid().unwrap().price, 79624.06,
            "infinity must NOT be returned as best bid");
        assert_eq!(ob.best_ask().unwrap().price, 79624.07);

        // All-invalid → None
        let all_bad = book(
            Exchange::Binance,
            vec![lvl(f64::NAN, 1.0), lvl(0.0, 1.0), lvl(-1.0, 1.0)],
            vec![lvl(f64::INFINITY, 1.0)],
        );
        assert!(all_bad.best_bid().is_none(), "all NaN/0/negative bids → None");
        // INFINITY is filtered too (not finite).
        assert!(all_bad.best_ask().is_none(), "INFINITY filtered → None");
    }

    /// `mid_price` and `spread` are unchanged in this commit but they
    /// delegate to `best_bid` / `best_ask`. Verify they pick up the
    /// fixed semantics correctly for a spot-exchange book.
    #[test]
    fn mid_price_and_spread_use_corrected_best() {
        let ob = book(
            Exchange::Binance,
            vec![lvl(79624.06, 1.0), lvl(79623.00, 1.0), lvl(79622.33, 1.0)],
            vec![lvl(79624.07, 1.0), lvl(79625.00, 1.0), lvl(79626.01, 1.0)],
        );
        // mid should be (79624.06 + 79624.07) / 2 = 79624.065,
        // NOT (79622.33 + 79626.01) / 2 = 79624.17 (pre-fix value).
        assert!((ob.mid_price() - 79624.065).abs() < 1e-6);
        // spread = 79624.07 - 79624.06 = 0.01, NOT 79626.01 - 79622.33 = 3.68.
        assert!((ob.spread().unwrap() - 0.01).abs() < 1e-6);
    }
}
