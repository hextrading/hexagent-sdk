//! Parquet-based market data replay reader.
//!
//! Reads Parquet files recorded by the writer, reconstructs MarketEvent objects,
//! and replays them ordered by local_timestamp_ns.

use anyhow::{anyhow, Result};
use arrow::array::*;
use chrono::{DateTime, Utc};
use log::{info, warn};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::path::{Path, PathBuf};

use crate::types::*;

/// A single row from a Parquet market data file, ready for replay.
struct ReplayRow {
    local_timestamp_ns: u64,
    event: MarketEvent,
}

/// Reads Parquet market data files and replays events in order.
/// Loads files on demand — only one file's data is held in memory at a time.
pub struct MarketReplayer {
    /// Sorted list of parquet files to replay.
    files: Vec<PathBuf>,
    /// Index of the next file to load.
    file_cursor: usize,
    /// Events from the currently loaded file, sorted by local_timestamp_ns.
    rows: Vec<ReplayRow>,
    /// Cursor within current file's rows.
    row_cursor: usize,
    start_ns: u64,
    end_ns: u64,
    event_count: u64,
}

impl MarketReplayer {
    /// Create a replayer from a directory or single Parquet file.
    /// Files are discovered and filtered by time range but NOT loaded yet.
    pub fn new(
        data_dir: &Path,
        exchange: &str,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Self> {
        let start_ns = start.timestamp_nanos_opt().unwrap_or(0) as u64;
        let end_ns = end.timestamp_nanos_opt().unwrap_or(0) as u64;

        // Discover Parquet files and filter by time range
        let all_files = discover_parquet_files(data_dir, exchange, symbol)?;
        let start_secs = start_ns / 1_000_000_000;
        let end_secs = end_ns / 1_000_000_000;
        let files: Vec<PathBuf> = all_files.into_iter().filter(|f| {
            match extract_file_timestamp(f) {
                Some((ts, duration)) => {
                    let file_end = ts + duration;
                    file_end > start_secs && ts < end_secs
                }
                None => true,
            }
        }).collect();

        if files.is_empty() {
            return Err(anyhow!("No Parquet files found for {}/{} in time range", exchange, symbol));
        }

        info!("[Replayer] Found {} Parquet files for {}/{}", files.len(), exchange, symbol);

        Ok(Self {
            files,
            file_cursor: 0,
            rows: Vec::new(),
            row_cursor: 0,
            start_ns,
            end_ns,
            event_count: 0,
        })
    }

    /// Load the next file's events into memory.
    fn load_next_file(&mut self) -> bool {
        while self.file_cursor < self.files.len() {
            let path = &self.files[self.file_cursor];
            self.file_cursor += 1;
            match read_parquet_events(path, self.start_ns, self.end_ns) {
                Ok(mut file_rows) => {
                    file_rows.sort_by_key(|r| r.local_timestamp_ns);
                    if !file_rows.is_empty() {
                        info!("[Replayer] Loaded {} rows from {}", file_rows.len(), path.display());
                        self.rows = file_rows;
                        self.row_cursor = 0;
                        return true;
                    }
                }
                Err(e) => {
                    // Zero-byte / truncated parquet files appear when the
                    // recorder creates the file but crashes / restarts
                    // before writing any rows (we saw 3 such files at
                    // `0514/20260514_04.parquet` for BTC/ETH/SOL on
                    // 2026-05-14). The size pre-check in
                    // `read_parquet_events` surfaces these as
                    // `empty parquet file (...)` so they're easy to grep
                    // for in production logs.
                    //
                    // `warn` (not `info`) because a corrupt / empty file
                    // is a real recorder pathology that the operator
                    // should notice; the predictor's warm-up loses that
                    // hour's training samples even though replay won't
                    // crash.
                    let msg = e.to_string();
                    if msg.starts_with("empty parquet file") {
                        warn!("[Replayer] Skip empty file {}: {}", path.display(), e);
                    } else {
                        info!("[Replayer] Skip {}: {}", path.display(), e);
                    }
                }
            }
        }
        false
    }

    /// Get next event with its recorded local timestamp, optionally simulating inter-event timing.
    pub fn next_event(&mut self) -> Result<Option<(u64, MarketEvent)>> {
        // If current file exhausted, load next
        if self.row_cursor >= self.rows.len() {
            if !self.load_next_file() {
                return Ok(None);
            }
        }

        // Take ownership of event instead of cloning (avoids heap allocation per event)
        let row = &mut self.rows[self.row_cursor];
        let ts = row.local_timestamp_ns;
        let event = std::mem::replace(&mut row.event, MarketEvent::Exit);
        self.row_cursor += 1;
        self.event_count += 1;

        Ok(Some((ts, event)))
    }

    pub fn event_count(&self) -> u64 {
        self.event_count
    }

    /// **Peek the first OrderBook mid for `symbol` at or after `target_ns`**
    /// in the still-unconsumed event stream (2026-05-21).
    ///
    /// Walks `self.rows[self.row_cursor..]` without advancing the cursor,
    /// looking for an `OrderBook` event whose symbol matches and whose
    /// `local_timestamp_ns >= target_ns`. Returns the OB's `mid_price()`
    /// on first hit, `None` if no such event before the current file
    /// ends. Cross-file peek is not supported — sufficient for the
    /// timeout-adverse-fill directional gate which only needs 1-2 s
    /// lookahead, and Polymarket per-event parquets cover ~5 min of OB
    /// per file.
    ///
    /// Cost: linear scan bounded by `max_scan` (we cap at 16 k rows ≈
    /// most-of-a-file). Safe to call on any tick without disturbing
    /// the event stream.
    pub fn peek_orderbook_mid_at(&self, symbol: &str, target_ns: u64) -> Option<f64> {
        const MAX_SCAN: usize = 16384;
        let end = (self.row_cursor + MAX_SCAN).min(self.rows.len());
        for i in self.row_cursor..end {
            let row = &self.rows[i];
            if row.local_timestamp_ns < target_ns { continue; }
            if let crate::types::MarketEvent::OrderBook(ob) = &row.event {
                if ob.symbol == symbol {
                    return Some(ob.mid_price());
                }
            }
        }
        None
    }

    /// **Peek the next full OrderBook snapshot for `symbol` strictly after
    /// `after_exch_ns`** (server/exchange axis) in the unconsumed stream
    /// (2026-05-30, sim_v2 one-step "race" model). Returns clones of the next
    /// book's `(bids, asks)`. Same bounded linear scan as
    /// `peek_orderbook_mid_at`; cross-file peek is not supported (Polymarket
    /// per-event parquets cover ~5 min — enough for a one-snapshot lookahead).
    pub fn peek_next_book(
        &self,
        symbol: &str,
        after_exch_ns: u64,
    ) -> Option<(u64, Vec<crate::types::PriceLevel>, Vec<crate::types::PriceLevel>)> {
        const MAX_SCAN: usize = 16384;
        let end = (self.row_cursor + MAX_SCAN).min(self.rows.len());
        for i in self.row_cursor..end {
            if let crate::types::MarketEvent::OrderBook(ob) = &self.rows[i].event {
                if ob.symbol == symbol && ob.exchange_timestamp_ns > after_exch_ns {
                    return Some((ob.exchange_timestamp_ns, ob.bids.clone(), ob.asks.clone()));
                }
            }
        }
        None
    }

    /// Like [`peek_next_book`] but returns BORROWED level slices (no clone) —
    /// for callers that only READ the next book (e.g. the forward-markout mid
    /// peek) rather than storing it. Identical selection to `peek_next_book`.
    pub fn peek_next_book_ref(
        &self,
        symbol: &str,
        after_exch_ns: u64,
    ) -> Option<(u64, &[crate::types::PriceLevel], &[crate::types::PriceLevel])> {
        const MAX_SCAN: usize = 16384;
        let end = (self.row_cursor + MAX_SCAN).min(self.rows.len());
        for i in self.row_cursor..end {
            if let crate::types::MarketEvent::OrderBook(ob) = &self.rows[i].event {
                if ob.symbol == symbol && ob.exchange_timestamp_ns > after_exch_ns {
                    return Some((ob.exchange_timestamp_ns, &ob.bids, &ob.asks));
                }
            }
        }
        None
    }

    /// **All OrderBook snapshots for `symbol` in the window `(after_ns, until_ns]`**
    /// (server/exchange axis), in stream order (2026-05-30, taker windowed race).
    /// Used to take the MIN fillable volume over an in-flight window rather than
    /// a single endpoint snapshot. Same bounded scan as `peek_next_book`.
    pub fn peek_books_in_window(
        &self,
        symbol: &str,
        after_ns: u64,
        until_ns: u64,
    ) -> Vec<(u64, Vec<crate::types::PriceLevel>, Vec<crate::types::PriceLevel>)> {
        const MAX_SCAN: usize = 16384;
        let end = (self.row_cursor + MAX_SCAN).min(self.rows.len());
        let mut out = Vec::new();
        for i in self.row_cursor..end {
            if let crate::types::MarketEvent::OrderBook(ob) = &self.rows[i].event {
                if ob.symbol == symbol
                    && ob.exchange_timestamp_ns > after_ns
                    && ob.exchange_timestamp_ns <= until_ns
                {
                    out.push((ob.exchange_timestamp_ns, ob.bids.clone(), ob.asks.clone()));
                }
            }
        }
        out
    }
}

/// Extract a Unix timestamp (seconds) and duration (seconds) from a parquet filename.
/// Returns (timestamp, duration_secs).
/// Handles: "btc-updown-5m-1774868400-321239.parquet" → Some((1774868400, 300))
///          "20260330_18.parquet" → Some((1774990800, 3600))
fn extract_file_timestamp(path: &Path) -> Option<(u64, u64)> {
    let stem = path.file_stem()?.to_str()?;
    // Try YYYYMMDD_HHMM format (5-minute files)
    if stem.len() == 13 && stem.contains('_') {
        let date_part = &stem[..8];
        let time_part = &stem[9..13];
        let date = chrono::NaiveDate::parse_from_str(date_part, "%Y%m%d").ok()?;
        let hour: u32 = time_part[..2].parse().ok()?;
        let minute: u32 = time_part[2..4].parse().ok()?;
        let dt = date.and_hms_opt(hour, minute, 0)?;
        return Some((dt.and_utc().timestamp() as u64, 300));
    }
    // Try YYYYMMDD_HH format (legacy hourly files)
    if stem.len() == 11 && stem.contains('_') {
        let date_part = &stem[..8];
        let hour_part = &stem[9..11];
        let date = chrono::NaiveDate::parse_from_str(date_part, "%Y%m%d").ok()?;
        let hour: u32 = hour_part.parse().ok()?;
        let dt = date.and_hms_opt(hour, 0, 0)?;
        return Some((dt.and_utc().timestamp() as u64, 3600));
    }
    // Try slug format: extract last numeric segment before event_id
    // e.g. "btc-updown-5m-1774868400-321239" → (1774868400, 300)
    let parts: Vec<&str> = stem.rsplitn(3, '-').collect();
    if parts.len() >= 2 {
        if let Ok(ts) = parts[1].parse::<u64>() {
            if ts > 1_700_000_000 {
                // Parse duration from slug: "5m" → 300, "15m" → 900, "1h" → 3600
                let slug_prefix = if parts.len() >= 3 { parts[2] } else { "" };
                let duration = slug_prefix.split('-')
                    .find_map(|p| {
                        p.strip_suffix('m').and_then(|n| n.parse::<u64>().ok().map(|n| n * 60))
                            .or_else(|| p.strip_suffix('h').and_then(|n| n.parse::<u64>().ok().map(|n| n * 3600)))
                    })
                    .unwrap_or(300); // default 5 min
                return Some((ts, duration));
            }
        }
    }
    None
}

/// Newest recorded `local_timestamp_ns` for a source — the LIVE
/// data-freshness pre-flight ([`crate::engine`]) uses this to measure the
/// gap between the last recorded orderbook/trade event and `now`.
///
/// Reads the TRUE last event from the most recent parquet file(s): the
/// newest file is often a partial / mid-window recording, so the
/// filename-embedded window end alone would under-report the gap (and a
/// safety gate must never under-report). Probes the 3 newest files
/// newest-first, falling back to the filename window-end if they can't be
/// read, and returns `None` when the source has no recorded files at all.
pub fn latest_recorded_ts_ns(data_dir: &Path, exchange: &str, symbol: &str) -> Option<u64> {
    let mut files = discover_parquet_files(data_dir, exchange, symbol).ok()?;
    if files.is_empty() {
        return None;
    }
    // Order by the filename-embedded window end so we probe newest-first.
    files.sort_by_key(|f| extract_file_timestamp(f).map(|(ts, dur)| ts + dur).unwrap_or(0));
    for path in files.iter().rev().take(3) {
        if let Ok(rows) = read_parquet_events(path, 0, u64::MAX) {
            if let Some(max_ts) = rows.iter().map(|r| r.local_timestamp_ns).max() {
                return Some(max_ts);
            }
        }
    }
    // All probed files empty/corrupt — fall back to the newest filename end.
    files
        .last()
        .and_then(|f| extract_file_timestamp(f))
        .map(|(ts, dur)| (ts + dur) * 1_000_000_000)
}

/// Discover .parquet files, optionally filtered by time range.
/// Files whose timestamp falls entirely outside [start_ns, end_ns) are skipped.
fn discover_parquet_files(data_dir: &Path, exchange: &str, symbol: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    // Strip "series:" prefix for file matching
    let match_slug = if symbol.starts_with("series:") {
        &symbol["series:".len()..]
    } else {
        symbol
    };

    // Pattern 1: {data_dir}/{exchange}/{symbol}/ directory with Parquet files (hourly)
    let symbol_dir = data_dir.join(exchange).join(match_slug);
    if symbol_dir.is_dir() {
        collect_parquet_recursive(&symbol_dir, &mut files)?;
    }

    // Pattern 2: {data_dir}/{exchange}/{event_id}_{slug}.parquet (event-based)
    // Only match loose parquet files whose name contains the slug.
    // Skip subdirectories — Pattern 1 already handles the matching directory.
    let exchange_dir = data_dir.join(exchange);
    if exchange_dir.is_dir() {
        if let Ok(entries) = std::fs::read_dir(&exchange_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() && path.extension().map(|e| e == "parquet").unwrap_or(false) {
                    let fname = path.file_stem().unwrap_or_default().to_string_lossy();
                    if fname.contains(match_slug) {
                        files.push(path);
                    }
                }
            }
        }
    }

    // Pattern 3: Direct file path
    let direct = PathBuf::from(symbol);
    if direct.exists() && direct.extension().map(|e| e == "parquet").unwrap_or(false) {
        files.push(direct);
    }

    files.sort();
    files.dedup();
    Ok(files)
}

fn collect_parquet_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_parquet_recursive(&path, files)?;
            } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                files.push(path);
            }
        }
    }
    Ok(())
}

/// Read a Parquet file and convert rows to ReplayRow events.
/// If no instrument row is found, synthesizes one from file metadata and market data symbols.
/// Parse a `[{"price":..,"quantity":..},..]` order-book level array.
///
/// The recorder writes these via `serde_json::to_string(&[PriceLevel])` →
/// compact, price-first, no whitespace. We parse that shape directly from the
/// borrowed `&str` with a tiny scalar parser ([`fast_parse_levels`]): no input
/// copy, no JSON tape, output `Vec` pre-sized — far less per-snapshot
/// allocation than the general path (`serde_json`/`simd-json` both copy +
/// build a tape + grow the Vec). This is the dominant replay-decode cost and a
/// source of allocation churn / memory pressure.
///
/// **Result-preserving**: numbers are parsed with std `f64::from_str`, which is
/// correctly-rounded — identical to serde_json / simd-json for any decimal they
/// emit. On ANY input the fast path doesn't recognise (unexpected shape / field
/// order / whitespace) it returns `None` and we fall back to simd-json (the
/// previously-shipped path), so the output is never wrong — only ever the fast
/// correct value or the proven fallback. Covered by
/// `custom_parser_matches_serde_json` below.
#[inline]
fn parse_price_levels(s: &str) -> Vec<PriceLevel> {
    if let Some(v) = fast_parse_levels(s.as_bytes()) {
        return v;
    }
    // Rare fallback (format drift): simd-json needs a mutable owned buffer.
    let mut buf = s.as_bytes().to_vec();
    simd_json::serde::from_slice::<Vec<PriceLevel>>(&mut buf).unwrap_or_default()
}

#[inline]
fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\n' | b'\r') {
        i += 1;
    }
    i
}

/// Match `key` (optional surrounding ws + the `:`), then parse the numeric token
/// up to the next `,`/`}`/ws as `f64`. `None` on any mismatch — the token is the
/// exact substring the recorder wrote, so `str::parse` round-trips it precisely.
fn parse_keyed_num(b: &[u8], i: &mut usize, key: &[u8]) -> Option<f64> {
    let mut j = skip_ws(b, *i);
    if j + key.len() > b.len() || &b[j..j + key.len()] != key {
        return None;
    }
    j = skip_ws(b, j + key.len());
    if j >= b.len() || b[j] != b':' {
        return None;
    }
    j = skip_ws(b, j + 1);
    let start = j;
    while j < b.len() && !matches!(b[j], b',' | b'}' | b' ' | b'\t' | b'\n' | b'\r') {
        j += 1;
    }
    if j == start {
        return None;
    }
    let val: f64 = std::str::from_utf8(&b[start..j]).ok()?.parse().ok()?;
    *i = j;
    Some(val)
}

/// Fast scalar parser for the recorder's compact
/// `[{"price":<num>,"quantity":<num>},...]` (price first). Returns `None` on ANY
/// deviation so the caller falls back to simd-json — it therefore only ever
/// returns values byte-for-byte equal to the general parse path.
fn fast_parse_levels(b: &[u8]) -> Option<Vec<PriceLevel>> {
    let n = b.len();
    let mut i = skip_ws(b, 0);
    if i >= n || b[i] != b'[' {
        return None;
    }
    i = skip_ws(b, i + 1);
    let mut out = Vec::with_capacity(8); // recorder caps depth at 5/side
    if i < n && b[i] == b']' {
        return if skip_ws(b, i + 1) == n { Some(out) } else { None };
    }
    loop {
        if i >= n || b[i] != b'{' {
            return None;
        }
        i += 1;
        let price = parse_keyed_num(b, &mut i, b"\"price\"")?;
        i = skip_ws(b, i);
        if i >= n || b[i] != b',' {
            return None;
        }
        i += 1;
        let quantity = parse_keyed_num(b, &mut i, b"\"quantity\"")?;
        i = skip_ws(b, i);
        if i >= n || b[i] != b'}' {
            return None;
        }
        out.push(PriceLevel { price, quantity });
        i = skip_ws(b, i + 1);
        if i >= n {
            return None;
        }
        match b[i] {
            b',' => i = skip_ws(b, i + 1),
            b']' => {
                i += 1;
                break;
            }
            _ => return None,
        }
    }
    if skip_ws(b, i) == n {
        Some(out)
    } else {
        None
    }
}

fn read_parquet_events(path: &Path, start_ns: u64, end_ns: u64) -> Result<Vec<ReplayRow>> {
    // Defensive size check before opening the parquet builder.
    // Zero-byte files appear at hour boundaries when the recorder
    // creates the file but crashes / restarts before writing any rows.
    // The arrow builder would still error ("Parquet file size is 0
    // bytes") but the typed error here surfaces the root cause cleanly
    // — and saves the syscall round-trip to mmap a footer that doesn't
    // exist.
    let md = std::fs::metadata(path)
        .map_err(|e| anyhow!("metadata({}): {}", path.display(), e))?;
    if md.len() == 0 {
        return Err(anyhow!("empty parquet file ({})", path.display()));
    }
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut rows = Vec::new();
    let mut has_instrument = false;
    let mut token_ids: Vec<String> = Vec::new();
    let mut min_ts = u64::MAX;

    for batch_result in reader {
        let batch = batch_result?;
        let n = batch.num_rows();

        let ts_col = batch.column_by_name("timestamp_ns")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let local_ts_col = batch.column_by_name("local_timestamp_ns")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let exchange_col = batch.column_by_name("exchange")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let etype_col = batch.column_by_name("event_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let symbol_col = batch.column_by_name("symbol")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let side_col = batch.column_by_name("side")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let price_col = batch.column_by_name("price")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let quantity_col = batch.column_by_name("quantity")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let bid_price_col = batch.column_by_name("bid_price")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ask_price_col = batch.column_by_name("ask_price")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let bid_qty_col = batch.column_by_name("bid_qty")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ask_qty_col = batch.column_by_name("ask_qty")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let bids_json_col = batch.column_by_name("bids_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let asks_json_col = batch.column_by_name("asks_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let data_json_col = batch.column_by_name("data_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let (ts_arr, local_ts_arr, exchange_arr, etype_arr, symbol_arr) =
            match (ts_col, local_ts_col, exchange_col, etype_col, symbol_col) {
                (Some(a), Some(b), Some(c), Some(d), Some(e)) => (a, b, c, d, e),
                _ => continue, // missing required columns
            };

        for i in 0..n {
            let local_ts = local_ts_arr.value(i);

            let exchange_ts = ts_arr.value(i);
            let exchange_str = exchange_arr.value(i);
            let event_type = etype_arr.value(i);

            // Time range filter (skip instruments — always load them for
            // token registration). Filter by `timestamp_ns` (event time)
            // rather than `local_timestamp_ns` (recorder receive time):
            //
            // - Live-recorded data has ts ≈ local_ts (within ms-level
            //   receive lag), so this is a no-op behavior change.
            // - Backfilled data (fix_usdtusd_data.py, fix_chainlink_
            //   boundaries.py) writes the historic event time into
            //   `timestamp_ns` but originally used `time.time_ns()` for
            //   `local_timestamp_ns`. When the BT window is in the past
            //   (e.g. 2026-05-08) and `local_ts` is "now" (2026-05-20),
            //   every backfilled row got filtered out, the strategy
            //   never saw the fx update, and no `[fx] usdt/usd` log
            //   fired. Filtering by event time fixes that semantically
            //   for any future-time-stamped backfill.
            if event_type != "instrument" && (exchange_ts < start_ns || exchange_ts >= end_ns) {
                continue;
            }
            let symbol = symbol_arr.value(i);

            let exchange = match exchange_str {
                "polymarket" => Exchange::Polymarket,
                "hexmarket" => Exchange::Hexmarket,
                "binance" => Exchange::Binance,
                "bybit" => Exchange::Bybit,
                "coinbase" => Exchange::Coinbase,
                "kraken" => Exchange::Kraken,
                "okx" => Exchange::Okx,
                "gate" => Exchange::Gate,
                "bitget" => Exchange::Bitget,
                "kucoin" => Exchange::Kucoin,
                "mexc" => Exchange::Mexc,
                "hyperliquid" => Exchange::Hyperliquid,
                "aster" => Exchange::Aster,
                "lighter" => Exchange::Lighter,
                _ => {
                    // RTDS spot_price events have source as exchange (e.g. "rtds_chainlink")
                    // Pass through for spot_price parsing — exchange field unused for SpotPrice
                    if event_type == "spot_price" {
                        Exchange::Polymarket // placeholder, SpotPrice uses source field
                    } else {
                        continue;
                    }
                }
            };

            // Track token IDs and min timestamp for synthetic instrument
            if matches!(event_type, "orderbook" | "trade") {
                if exchange == Exchange::Polymarket && !token_ids.contains(&symbol.to_string()) {
                    token_ids.push(symbol.to_string());
                }
                if local_ts < min_ts {
                    min_ts = local_ts;
                }
            }

            let event = match event_type {
                "orderbook" => {
                    let bids = bids_json_col
                        .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                        .map(parse_price_levels)
                        .unwrap_or_default();
                    let asks = asks_json_col
                        .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                        .map(parse_price_levels)
                        .unwrap_or_default();

                    MarketEvent::OrderBook(OrderBookSnapshot {
                        exchange,
                        symbol: symbol.to_string(),
                        bids,
                        asks,
                        exchange_timestamp_ns: exchange_ts,
                        local_timestamp_ns: local_ts,
                    })
                }
                "trade" => {
                    let price = price_col.map(|c| c.value(i)).unwrap_or(0.0);
                    let quantity = quantity_col.map(|c| c.value(i)).unwrap_or(0.0);
                    let side_str = side_col.and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
                    let side = match side_str {
                        Some("buy") => Side::Buy,
                        _ => Side::Sell,
                    };

                    MarketEvent::Trade(TradeTick {
                        exchange,
                        symbol: symbol.to_string(),
                        price,
                        quantity,
                        side,
                        exchange_timestamp_ns: exchange_ts,
                        local_timestamp_ns: local_ts,
                    })
                }
                "quote" => {
                    MarketEvent::Quote(QuoteTick {
                        exchange,
                        symbol: symbol.to_string(),
                        bid_price: bid_price_col.map(|c| c.value(i)).unwrap_or(0.0),
                        bid_qty: bid_qty_col.map(|c| c.value(i)).unwrap_or(0.0),
                        ask_price: ask_price_col.map(|c| c.value(i)).unwrap_or(0.0),
                        ask_qty: ask_qty_col.map(|c| c.value(i)).unwrap_or(0.0),
                        exchange_timestamp_ns: exchange_ts,
                        local_timestamp_ns: local_ts,
                    })
                }
                "tick_size_change" => {
                    MarketEvent::TickSizeChange(TickSizeChange {
                        exchange,
                        symbol: symbol.to_string(),
                        old_tick_size: quantity_col.map(|c| c.value(i)).unwrap_or(0.0),
                        new_tick_size: price_col.map(|c| c.value(i)).unwrap_or(0.0),
                        local_timestamp_ns: local_ts,
                    })
                }
                // `spot_price_proxy` is the legacy event_type written
                // by the recorder for derived/computed spot feeds (e.g.
                // binance_futures USDTUSD@assetIndex). Both names map
                // to MarketEvent::SpotPrice — without this alias the
                // strategy never sees those rows during BT replay
                // (silently skipped, leaving usdt_price stuck at 1.0).
                "spot_price" | "spot_price_proxy" => {
                    let price = price_col.map(|c| c.value(i)).unwrap_or(0.0);
                    MarketEvent::SpotPrice(SpotPrice {
                        source: exchange_str.to_string(),
                        symbol: symbol.to_string(),
                        price,
                        timestamp_ns: exchange_ts,
                        local_timestamp_ns: local_ts,
                    })
                }
                // Perp asset-context rows (Hyperliquid activeAssetCtx):
                // mark px in `price`, impact bid/ask in `bid_price`/
                // `ask_price`, the remaining ctx fields as compact JSON in
                // `data_json` (see writer::push_asset_ctx). Reconstructed so
                // BT replay delivers funding/oracle to `on_asset_ctx`.
                "asset_ctx" => {
                    let j: serde_json::Value = data_json_col
                        .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) })
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(serde_json::Value::Null);
                    let f = |k: &str| j.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
                    MarketEvent::AssetCtx(crate::types::AssetCtxTick {
                        exchange,
                        symbol: symbol.to_string(),
                        mark_px: price_col.map(|c| c.value(i)).unwrap_or(0.0),
                        oracle_px: f("oraclePx"),
                        mid_px: f("midPx"),
                        funding: f("funding"),
                        open_interest: f("openInterest"),
                        premium: f("premium"),
                        impact_bid_px: bid_price_col.map(|c| c.value(i)).unwrap_or(0.0),
                        impact_ask_px: ask_price_col.map(|c| c.value(i)).unwrap_or(0.0),
                        day_ntl_vlm: f("dayNtlVlm"),
                        prev_day_px: f("prevDayPx"),
                        local_timestamp_ns: local_ts,
                    })
                }
                "instrument" => {
                    has_instrument = true;
                    // Reconstruct from data_json
                    let json_str = data_json_col
                        .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
                    if let Some(json) = json_str {
                        if let Ok(evt) = serde_json::from_str::<MarketEvent>(json) {
                            evt
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                _ => continue,
            };

            rows.push(ReplayRow { local_timestamp_ns: local_ts, event });
        }
    }

    if !has_instrument && path.to_string_lossy().contains("polymarket") {
        warn!("[Replayer] No instrument in {}", path.display());
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The custom order-book parser must produce **bit-identical** `f64` output
    /// to `serde_json::from_str::<Vec<PriceLevel>>` for every input — otherwise
    /// the parse change would alter backtest fills. Compares via `to_bits()`.
    #[test]
    fn custom_parser_matches_serde_json() {
        let cases = [
            r#"[{"price":0.52,"quantity":100.0},{"price":0.51,"quantity":250.5}]"#,
            r#"[{"price":0.999,"quantity":1.0},{"price":0.001,"quantity":1000000.0}]"#,
            r#"[{"price":1,"quantity":0}]"#,                 // integer literals
            r#"[{"price":6.1e-2,"quantity":1.25e3}]"#,        // scientific notation
            r#"[{"price":-0.0,"quantity":12.5}]"#,            // signed zero
            r#"[]"#,                                          // empty book side
            r#"[{"price":0.6612345678901234,"quantity":0.1}]"#, // long mantissa (rounding)
            r#" [ {"price": 0.5 , "quantity": 3.0 } ] "#,     // whitespace → fallback path
            r#"[{"quantity":3.0,"price":0.5}]"#,              // field order reversed → fallback
            r#"not json"#,                                    // malformed → empty
            r#"[{"price":0.5}]"#,                             // missing field → fallback→empty
        ];
        for c in cases {
            let got = parse_price_levels(c);
            let serde = serde_json::from_str::<Vec<PriceLevel>>(c).unwrap_or_default();
            assert_eq!(got.len(), serde.len(), "len mismatch for {c}");
            for (a, b) in got.iter().zip(serde.iter()) {
                assert_eq!(a.price.to_bits(), b.price.to_bits(), "price bits differ for {c}");
                assert_eq!(a.quantity.to_bits(), b.quantity.to_bits(), "qty bits differ for {c}");
            }
        }
    }

    /// The EXACT recorder path: values serialised by `serde_json::to_string`
    /// (what `writer.rs` writes) must parse back bit-identically vs serde_json.
    #[test]
    fn custom_parser_roundtrips_recorder_format() {
        let books: Vec<Vec<PriceLevel>> = vec![
            vec![PriceLevel { price: 0.523, quantity: 100.0 }, PriceLevel { price: 0.517, quantity: 250.5 }],
            vec![PriceLevel { price: 0.0001, quantity: 1_000_000.0 }, PriceLevel { price: 0.9999, quantity: 0.01 }],
            vec![PriceLevel { price: 1.0 / 3.0, quantity: 7.0 / 11.0 }], // non-terminating decimals
            vec![],
        ];
        for b in &books {
            let s = serde_json::to_string(b).unwrap();           // recorder's exact output
            let got = parse_price_levels(&s);
            let serde = serde_json::from_str::<Vec<PriceLevel>>(&s).unwrap_or_default();
            assert_eq!(got.len(), serde.len(), "len mismatch for {s}");
            for (x, y) in got.iter().zip(serde.iter()) {
                assert_eq!(x.price.to_bits(), y.price.to_bits(), "price bits differ for {s}");
                assert_eq!(x.quantity.to_bits(), y.quantity.to_bits(), "qty bits differ for {s}");
                // and equal to the ORIGINAL f64 (round-trip)
            }
            for (x, orig) in got.iter().zip(b.iter()) {
                assert_eq!(x.price.to_bits(), orig.price.to_bits(), "price not round-tripped for {s}");
                assert_eq!(x.quantity.to_bits(), orig.quantity.to_bits(), "qty not round-tripped for {s}");
            }
        }
    }

    /// Zero-byte parquet files (recorder pathology — see
    /// `data/binance/{BTC,ETH,SOL}USDT/202605/0514/20260514_04.parquet`
    /// on 2026-05-14) must produce a typed error before reaching the
    /// arrow builder, so callers can grep for `empty parquet file` in
    /// production logs and the predictor warm-up logs a clear skip
    /// instead of a generic "Parquet file size is 0 bytes" surfaced
    /// from a library deep in the stack.
    #[test]
    fn read_parquet_events_rejects_zero_byte_file() {
        let dir = std::env::temp_dir().join(format!("hexbot_empty_pq_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("zero.parquet");
        std::fs::File::create(&path).unwrap();   // 0 bytes
        let err = match read_parquet_events(&path, 0, u64::MAX) {
            Err(e) => e,
            Ok(_) => panic!("must error on zero-byte parquet"),
        };
        let msg = err.to_string();
        assert!(
            msg.starts_with("empty parquet file"),
            "expected `empty parquet file ...`, got: {}", msg,
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
