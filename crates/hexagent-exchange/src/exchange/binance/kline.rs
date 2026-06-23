//! Binance REST API kline (candlestick) fetcher.
//!
//! Fetches historical klines from Binance API and returns BarData.
//! API docs: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data

use anyhow::{anyhow, Result};
use log::info;

use crate::types::{BarData, Exchange};

const BINANCE_API_BASE: &str = "https://api.binance.com";
const MAX_KLINES_PER_REQUEST: u64 = 1000;

/// Convert interval string to milliseconds.
fn interval_to_ms(interval: &str) -> Result<u64> {
    match interval {
        // Sub-minute klines (Binance Spot supports `1s` natively
        // via /api/v3/klines since Jan 2024; same for WS
        // `<symbol>@kline_1s`). `5s` / `10s` aren't standard Binance
        // intervals but listed here for parity with hist_reader's
        // local-parquet support — REST fetch will 400 on them, but
        // local-first reads still work.
        "1s" => Ok(1_000),
        "5s" => Ok(5_000),
        "10s" => Ok(10_000),
        "1m" => Ok(60_000),
        "3m" => Ok(180_000),
        "5m" => Ok(300_000),
        "15m" => Ok(900_000),
        "30m" => Ok(1_800_000),
        "1h" => Ok(3_600_000),
        "2h" => Ok(7_200_000),
        "4h" => Ok(14_400_000),
        "1d" => Ok(86_400_000),
        _ => Err(anyhow!("Unsupported kline interval: {}", interval)),
    }
}

/// Fetch historical klines from Binance REST API.
///
/// Automatically paginates if the requested range exceeds 1000 bars.
/// Returns bars sorted by open_time ascending, all within [start_ns, end_ns).
pub fn fetch_klines(
    symbol: &str,
    interval: &str,
    start_ns: u64,
    end_ns: u64,
) -> Result<Vec<BarData>> {
    let interval_ms = interval_to_ms(interval)?;
    let mut all_bars = Vec::new();
    let mut cursor_ms = start_ns / 1_000_000; // ns → ms
    let end_ms = end_ns / 1_000_000;

    while cursor_ms < end_ms {
        let limit = ((end_ms - cursor_ms) / interval_ms + 1).min(MAX_KLINES_PER_REQUEST);
        let url = format!(
            "{}/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={}",
            BINANCE_API_BASE, symbol, interval, cursor_ms, end_ms - 1, limit,
        );

        // Route through the shared async runtime + reqwest client so
        // kline fetches reuse the process-wide h2 connection pool and
        // TLS sessions.
        let body = crate::async_rt::blocking_get_text(&url)
            .map_err(|e| anyhow!("Binance kline API error: {}", e))?;
        let resp: serde_json::Value = serde_json::from_str(&body)?;

        let arr = resp.as_array()
            .ok_or_else(|| anyhow!("Binance kline API: expected array"))?;

        if arr.is_empty() {
            break;
        }

        let mut batch_count = 0u64;
        for item in arr {
            let a = item.as_array()
                .ok_or_else(|| anyhow!("Binance kline: expected array row"))?;
            if a.len() < 11 {
                continue;
            }

            let open_time_ms = a[0].as_u64().unwrap_or(0);
            let close_time_ms = a[6].as_u64().unwrap_or(0);
            let open_time_ns = open_time_ms * 1_000_000;
            let close_time_ns = close_time_ms * 1_000_000;

            // Filter within [start_ns, end_ns)
            if open_time_ns < start_ns || open_time_ns >= end_ns {
                cursor_ms = open_time_ms + interval_ms;
                continue;
            }

            let bar = BarData {
                exchange: Exchange::Binance,
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                open_time_ns,
                close_time_ns,
                open: parse_str_f64(&a[1])?,
                high: parse_str_f64(&a[2])?,
                low: parse_str_f64(&a[3])?,
                close: parse_str_f64(&a[4])?,
                volume: parse_str_f64(&a[5])?,
                // Binance REST kline: [7]=quote_asset_volume,
                // [9]=taker_buy_base_asset_volume.
                quote_volume: a.get(7).and_then(|s| parse_str_f64(s).ok()).unwrap_or(0.0),
                taker_buy_base: a.get(9).and_then(|s| parse_str_f64(s).ok()).unwrap_or(0.0),
                is_closed: true,
                exchange_timestamp_ns: close_time_ns,
                local_timestamp_ns: close_time_ns,
            };

            cursor_ms = open_time_ms + interval_ms;
            all_bars.push(bar);
            batch_count += 1;
        }

        info!(
            "[BinanceKline] Fetched {} bars for {} {} (cursor={})",
            batch_count, symbol, interval, cursor_ms,
        );

        // If we got fewer than limit, we've reached the end
        if (arr.len() as u64) < limit {
            break;
        }
    }

    Ok(all_bars)
}

fn parse_str_f64(val: &serde_json::Value) -> Result<f64> {
    val.as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| val.as_f64())
        .ok_or_else(|| anyhow!("Cannot parse f64 from {:?}", val))
}
