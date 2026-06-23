//! Historical kline reader with local-first + API fallback.
//!
//! 1. Read local parquet files for the requested range
//! 2. Detect gaps (missing bars) in the local data
//! 3. Fetch missing bars from exchange API
//! 4. Save fetched bars to local parquet files
//! 5. Return complete, continuous bar data

use anyhow::{anyhow, Result};
use arrow::array::{Array, Float64Array, Int64Array, TimestampNanosecondArray, UInt32Array, UInt64Array};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use log::{info, warn};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::{Path, PathBuf};

use crate::types::{BarData, Exchange, HistDataRequest};

/// Interval string to nanoseconds.
fn interval_to_ns(interval: &str) -> Result<u64> {
    match interval {
        "1s" => Ok(1_000_000_000),
        "5s" => Ok(5_000_000_000),
        "10s" => Ok(10_000_000_000),
        "1m" => Ok(60_000_000_000),
        "3m" => Ok(180_000_000_000),
        "5m" => Ok(300_000_000_000),
        "15m" => Ok(900_000_000_000),
        "30m" => Ok(1_800_000_000_000),
        "1h" => Ok(3_600_000_000_000),
        "2h" => Ok(7_200_000_000_000),
        "4h" => Ok(14_400_000_000_000),
        "1d" => Ok(86_400_000_000_000),
        _ => Err(anyhow!("Unsupported interval: {}", interval)),
    }
}

/// Load historical bars for a request: local-first with API fallback.
///
/// File layout: `{data_dir}/histdata/{exchange}/{symbol}/{interval}/{YYYYMM}/{YYYYMMDD}.parquet`
///
/// Returns bars sorted by open_time ascending, guaranteed continuous within [start, end).
pub fn load_hist_bars(data_dir: &Path, req: &HistDataRequest) -> Result<Vec<BarData>> {
    let interval_ns = interval_to_ns(&req.interval)?;
    let exchange_str = req.exchange.to_string();
    let hist_dir = data_dir
        .join("histdata")
        .join(&exchange_str)
        .join(&req.symbol)
        .join(&req.interval);

    // Step 1: Read local data
    let mut local_bars = if hist_dir.is_dir() {
        let start_dt = DateTime::<Utc>::from_timestamp_nanos(req.start_date_ns as i64);
        let end_dt = DateTime::<Utc>::from_timestamp_nanos(req.end_date_ns as i64);
        let files = discover_parquet_files(&hist_dir, start_dt.date_naive(), end_dt.date_naive())?;

        let mut bars = Vec::new();
        for path in &files {
            match read_parquet_file(path, req.exchange, &req.interval, req.start_date_ns as i64, req.end_date_ns as i64) {
                Ok(mut file_bars) => bars.append(&mut file_bars),
                Err(e) => warn!("[HistReader] Skip {}: {}", path.display(), e),
            }
        }
        bars.sort_by_key(|b| b.open_time_ns);
        bars.dedup_by_key(|b| b.open_time_ns);
        bars
    } else {
        Vec::new()
    };

    info!(
        "[HistReader] Local: {} bars for {}/{} {} [{} → {}]",
        local_bars.len(),
        req.exchange,
        req.symbol,
        req.interval,
        fmt_ns(req.start_date_ns),
        fmt_ns(req.end_date_ns),
    );

    // Step 2: Find gaps
    let gaps = find_gaps(&local_bars, req.start_date_ns, req.end_date_ns, interval_ns);

    if gaps.is_empty() {
        return Ok(local_bars);
    }

    info!(
        "[HistReader] {} gaps detected, fetching from API...",
        gaps.len()
    );

    // Step 3: Fetch missing bars from API
    let fetched = fetch_missing_bars(req, &gaps)?;

    if !fetched.is_empty() {
        info!("[HistReader] Fetched {} bars from API", fetched.len());

        // Step 4: Save fetched bars to local parquet
        if let Err(e) = save_bars_to_local(&hist_dir, &fetched, &req.interval) {
            warn!("[HistReader] Failed to save bars locally: {}", e);
        }

        // Step 5: Merge
        local_bars.extend(fetched);
        local_bars.sort_by_key(|b| b.open_time_ns);
        local_bars.dedup_by_key(|b| b.open_time_ns);
    }

    info!(
        "[HistReader] Total: {} bars for {}/{} {}",
        local_bars.len(),
        req.exchange,
        req.symbol,
        req.interval,
    );

    Ok(local_bars)
}

/// Find gap ranges [start_ns, end_ns) where bars are missing.
fn find_gaps(bars: &[BarData], start_ns: u64, end_ns: u64, interval_ns: u64) -> Vec<(u64, u64)> {
    let mut gaps = Vec::new();
    // Align start to interval boundary
    let aligned_start = (start_ns / interval_ns) * interval_ns;
    let mut expected = aligned_start;

    for bar in bars {
        if bar.open_time_ns > expected {
            gaps.push((expected, bar.open_time_ns));
        }
        expected = bar.open_time_ns + interval_ns;
    }

    // Trailing gap
    if expected < end_ns {
        gaps.push((expected, end_ns));
    }

    gaps
}

/// Fetch bars from exchange API for the given gaps.
fn fetch_missing_bars(req: &HistDataRequest, gaps: &[(u64, u64)]) -> Result<Vec<BarData>> {
    // Note: as of 2024-01, Binance Spot's REST `/api/v3/klines` natively
    // supports `interval=1s` (and the WS variant `<symbol>@kline_1s`).
    // The pre-2026-05-20 graceful-skip for sub-minute intervals has
    // been removed — the same `fetch_klines` path now handles 1s gaps,
    // so startup and reconnect gap-fill work via the existing
    // local-first + API-fallback machinery. `5s` / `10s` are still not
    // standard Binance intervals; the API will return 400 for those,
    // but local parquet reads continue to work.
    let mut all_bars = Vec::new();

    for &(gap_start, gap_end) in gaps {
        let bars = match req.exchange {
            Exchange::Binance => {
                crate::exchange::binance::fetch_klines(
                    &req.symbol,
                    &req.interval,
                    gap_start,
                    gap_end,
                )?
            }
            _ => {
                warn!(
                    "[HistReader] No API fetcher for exchange {:?}, skipping gap",
                    req.exchange
                );
                Vec::new()
            }
        };
        all_bars.extend(bars);
    }

    Ok(all_bars)
}

/// Save bars to local parquet files, grouped by date.
/// File path: `{hist_dir}/{YYYYMM}/{YYYYMMDD}.parquet`
pub fn save_bars_to_local(hist_dir: &Path, bars: &[BarData], interval: &str) -> Result<()> {
    use arrow::array::{ArrayRef, Float64Builder, StringBuilder, TimestampNanosecondBuilder};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    // Group bars by date
    let mut by_date: BTreeMap<NaiveDate, Vec<&BarData>> = BTreeMap::new();
    for bar in bars {
        let dt = DateTime::<Utc>::from_timestamp_nanos(bar.open_time_ns as i64);
        by_date.entry(dt.date_naive()).or_default().push(bar);
    }

    for (date, day_bars) in &by_date {
        let month_str = format!("{}{:02}", date.year(), date.month());
        let day_str = format!("{}{:02}{:02}", date.year(), date.month(), date.day());
        let dir = hist_dir.join(&month_str);
        std::fs::create_dir_all(&dir)?;

        let file_path = dir.join(format!("{}.parquet", day_str));

        // If file exists, read existing bars, merge, and rewrite
        let mut merged = Vec::new();
        if file_path.exists() {
            if let Ok(existing) = read_parquet_file(&file_path, day_bars[0].exchange, interval, 0, i64::MAX) {
                merged.extend(existing);
            }
        }
        for b in day_bars {
            merged.push((*b).clone());
        }
        merged.sort_by_key(|b| b.open_time_ns);
        merged.dedup_by_key(|b| b.open_time_ns);

        // Build arrow arrays
        let schema = Arc::new(Schema::new(vec![
            Field::new("Open time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("Close time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("Open", DataType::Float64, false),
            Field::new("High", DataType::Float64, false),
            Field::new("Low", DataType::Float64, false),
            Field::new("Close", DataType::Float64, false),
            Field::new("Volume", DataType::Float64, false),
            // Order-flow columns (snake_case to match the downloaded 1s
            // histdata schema + what the reader looks up by exact name) so
            // live-recorded archives carry buy/quote volume for uniform
            // processing by the residual-model features.
            Field::new("taker_buy_base", DataType::Float64, false),
            Field::new("quote_volume", DataType::Float64, false),
            Field::new("interval", DataType::Utf8, false),
        ]));

        let n = merged.len();
        let mut open_time = TimestampNanosecondBuilder::with_capacity(n);
        let mut close_time = TimestampNanosecondBuilder::with_capacity(n);
        let mut open = Float64Builder::with_capacity(n);
        let mut high = Float64Builder::with_capacity(n);
        let mut low = Float64Builder::with_capacity(n);
        let mut close = Float64Builder::with_capacity(n);
        let mut volume = Float64Builder::with_capacity(n);
        let mut taker_buy = Float64Builder::with_capacity(n);
        let mut quote_vol = Float64Builder::with_capacity(n);
        let mut interval_col = StringBuilder::with_capacity(n, n * 2);

        for bar in &merged {
            open_time.append_value(bar.open_time_ns as i64);
            close_time.append_value(bar.close_time_ns as i64);
            open.append_value(bar.open);
            high.append_value(bar.high);
            low.append_value(bar.low);
            close.append_value(bar.close);
            volume.append_value(bar.volume);
            taker_buy.append_value(bar.taker_buy_base);
            quote_vol.append_value(bar.quote_volume);
            interval_col.append_value(&bar.interval);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(open_time.finish()) as ArrayRef,
                Arc::new(close_time.finish()) as ArrayRef,
                Arc::new(open.finish()) as ArrayRef,
                Arc::new(high.finish()) as ArrayRef,
                Arc::new(low.finish()) as ArrayRef,
                Arc::new(close.finish()) as ArrayRef,
                Arc::new(volume.finish()) as ArrayRef,
                Arc::new(taker_buy.finish()) as ArrayRef,
                Arc::new(quote_vol.finish()) as ArrayRef,
                Arc::new(interval_col.finish()) as ArrayRef,
            ],
        )?;

        let file = File::create(&file_path)?;
        // SNAPPY compression: ~5–6× size reduction on hist-bar schema,
        // same rationale as `MarketRecorder` (see writer.rs comment).
        // Parquet crate default with `None` properties is UNCOMPRESSED.
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        info!(
            "[HistReader] Saved {} bars to {}",
            merged.len(),
            file_path.display()
        );
    }

    Ok(())
}

fn fmt_ns(ns: u64) -> String {
    DateTime::<Utc>::from_timestamp_nanos(ns as i64)
        .format("%Y-%m-%d %H:%M")
        .to_string()
}

// ── Local parquet reading ─────────────────────────────────────────────

/// Discover `.parquet` files under `hist_dir/{YYYYMM}/{YYYYMMDD}.parquet`
/// whose date falls within `[start_date, end_date]`, sorted by name.
fn discover_parquet_files(
    hist_dir: &Path,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = Vec::new();

    let mut month_dirs: Vec<_> = std::fs::read_dir(hist_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .collect();
    month_dirs.sort_by_key(|e| e.file_name());

    for month_entry in month_dirs {
        let month_path = month_entry.path();
        let mut entries: Vec<_> = std::fs::read_dir(&month_path)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map_or(false, |ext| ext == "parquet")
            })
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            if let Some(file_date) = parse_filename_date(&path) {
                if file_date >= start_date && file_date <= end_date {
                    files.push(path);
                }
            } else {
                files.push(path);
            }
        }
    }

    Ok(files)
}

fn parse_filename_date(path: &Path) -> Option<NaiveDate> {
    let stem = path.file_stem()?.to_str()?;
    NaiveDate::parse_from_str(stem, "%Y%m%d").ok()
}

/// Read a single parquet file and return BarData rows filtered by [start_ns, end_ns).
fn read_parquet_file(
    path: &Path,
    exchange: Exchange,
    interval: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<BarData>> {
    let symbol = path
        .parent()                    // {YYYYMM}/
        .and_then(|p| p.parent())    // {interval}/
        .and_then(|p| p.parent())    // {symbol}/
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("Cannot extract symbol from path: {}", path.display()))?;
    // Zero-byte guard — matches `recorder::reader::read_parquet_events`.
    // Predicted-vol/intensity warm-up uses these bars; surface a clean
    // error instead of the arrow library's "Parquet file size is 0"
    // when the recorder leaves an empty placeholder at an hour
    // boundary.
    let md = std::fs::metadata(path)
        .map_err(|e| anyhow!("metadata({}): {}", path.display(), e))?;
    if md.len() == 0 {
        return Err(anyhow!("empty parquet file ({})", path.display()));
    }
    let file = File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut bars = Vec::new();

    for batch_result in reader {
        let batch = batch_result?;
        let num_rows = batch.num_rows();

        let schema = batch.schema();
        // Two parquet schemas in the wild:
        //
        //   Legacy (1m bars, hand-encoded TitleCase mirroring Binance's
        //   klines CSV header): "Open time" / "Open" / "High" / "Low" /
        //   "Close" / "Volume" / "Close time", `Timestamp(ns)` type for
        //   the two time cols.
        //
        //   Sub-minute (1s bars, 2026-05+): snake_case `timestamp_ns` /
        //   `open` / `high` / `low` / `close` / `volume` / `close_time_ns`,
        //   `UInt64` type for the two time cols, plus extra columns
        //   (`quote_volume`, `n_trades`, `taker_buy_base`, ...) we ignore.
        //
        // Try the legacy name first, fall back to snake_case. Surfacing
        // the failure mentions BOTH names so an operator with a third
        // unrecognised schema can see what's expected.
        let col_idx = |legacy: &str, snake: &str| -> Result<usize> {
            schema.index_of(legacy)
                .or_else(|_| schema.index_of(snake))
                .map_err(|_| anyhow!(
                    "Column '{}' / '{}' not found in {}", legacy, snake, path.display(),
                ))
        };

        let open_time_idx  = col_idx("Open time",  "timestamp_ns")?;
        let close_time_idx = col_idx("Close time", "close_time_ns")?;
        let open_idx       = col_idx("Open",   "open")?;
        let high_idx       = col_idx("High",   "high")?;
        let low_idx        = col_idx("Low",    "low")?;
        let close_idx      = col_idx("Close",  "close")?;
        let volume_idx     = col_idx("Volume", "volume")?;

        let open_times  = as_timestamp_ns(batch.column(open_time_idx),  "open_time",  path)?;
        let close_times = as_timestamp_ns(batch.column(close_time_idx), "close_time", path)?;

        let opens   = as_f64_array(batch.column(open_idx),   "open",   path)?;
        let highs   = as_f64_array(batch.column(high_idx),   "high",   path)?;
        let lows    = as_f64_array(batch.column(low_idx),    "low",    path)?;
        let closes  = as_f64_array(batch.column(close_idx),  "close",  path)?;
        let volumes = as_f64_array(batch.column(volume_idx), "volume", path)?;
        // Optional order-flow columns: 1s snake_case parquet carries
        // `taker_buy_base` + `quote_volume`; legacy 1m omits them → None →
        // 0.0. Feed the residual-model |OFI| / |VWAP−close| features.
        let taker_buys = schema.index_of("taker_buy_base").ok()
            .and_then(|idx| as_f64_array(batch.column(idx), "taker_buy_base", path).ok());
        let quote_vols = schema.index_of("quote_volume").ok()
            .and_then(|idx| as_f64_array(batch.column(idx), "quote_volume", path).ok());

        for i in 0..num_rows {
            let open_time_ns = open_times[i];
            let close_time_ns = close_times[i];

            if open_time_ns < start_ns || open_time_ns >= end_ns {
                continue;
            }

            bars.push(BarData {
                exchange,
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                open_time_ns: open_time_ns as u64,
                close_time_ns: close_time_ns as u64,
                open: opens[i],
                high: highs[i],
                low: lows[i],
                close: closes[i],
                volume: volumes[i],
                taker_buy_base: taker_buys.as_ref().map(|a| a[i]).unwrap_or(0.0),
                quote_volume: quote_vols.as_ref().map(|a| a[i]).unwrap_or(0.0),
                is_closed: true,
                exchange_timestamp_ns: close_time_ns as u64,
                local_timestamp_ns: close_time_ns as u64,
            });
        }
    }

    Ok(bars)
}

fn as_timestamp_ns(col: &dyn Array, name: &str, path: &Path) -> Result<Vec<i64>> {
    // Timestamp(Nanosecond) is the legacy 1m-bar encoding.
    if let Some(arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok((0..arr.len()).map(|i| arr.value(i)).collect());
    }
    // UInt64(ns) is the sub-minute 1s-bar encoding (column names
    // `timestamp_ns` / `close_time_ns`). Cast straight through —
    // bits never exceed i64 since 1e18 ns fits in 2^63.
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok((0..arr.len()).map(|i| arr.value(i) as i64).collect());
    }
    // Int64(ms) is the historical "Binance klines as raw millis"
    // encoding that some older writers used — multiply to ns.
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..arr.len()).map(|i| arr.value(i) * 1_000_000).collect());
    }
    Err(anyhow!(
        "Column '{}' is not Timestamp(ns) / UInt64(ns) / Int64(ms) in {}",
        name,
        path.display()
    ))
}

fn as_f64_array(col: &dyn Array, name: &str, path: &Path) -> Result<Vec<f64>> {
    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
        return Ok((0..arr.len()).map(|i| arr.value(i)).collect());
    }
    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok((0..arr.len()).map(|i| arr.value(i) as f64).collect());
    }
    // n_trades style integer columns appear as UInt32 in the snake_case
    // schema; map to f64 for symmetry even though we don't currently
    // surface them.
    if let Some(arr) = col.as_any().downcast_ref::<UInt32Array>() {
        return Ok((0..arr.len()).map(|i| arr.value(i) as f64).collect());
    }
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok((0..arr.len()).map(|i| arr.value(i) as f64).collect());
    }
    Err(anyhow!(
        "Column '{}' is not Float64 / Int64 / UInt32 / UInt64 in {}",
        name,
        path.display()
    ))
}
