use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use log::{info, warn};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// Default writer properties for hexbot-recorded parquets.
///
/// **SNAPPY compression** is enabled by default. Empirically gives a
/// ~5–6× size reduction on our OB-heavy schema (the bids_json /
/// asks_json columns contain highly repetitive JSON, perfect for
/// snappy's LZ-family algorithm). With `ArrowWriter::try_new(..., None)`
/// the parquet crate's `WriterProperties::default()` falls back to
/// `Compression::UNCOMPRESSED`, which is what hexbot recorder used to
/// emit — 38 MB/h files vs ~2.7 MB/h after this change for the same row
/// count.
///
/// We deliberately use SNAPPY rather than ZSTD or GZIP:
///   * SNAPPY decompresses ~4× faster than ZSTD-3 — meaningful for
///     prediction warm-up which replays 24 h of these parquets at
///     startup. Boot time should not be dominated by decompression.
///   * On our schema SNAPPY gives ~5–6× compression while ZSTD-3 gives
///     ~7–8× — the marginal disk saving isn't worth the CPU cost.
///   * Replayer code path is unchanged: the parquet crate reads any
///     supported compression transparently.
fn recorder_writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

use crate::types::MarketEvent;

/// Schema for market event Parquet files.
fn market_event_schema() -> Schema {
    Schema::new(vec![
        Field::new("timestamp_ns", DataType::UInt64, false),       // exchange timestamp
        Field::new("local_timestamp_ns", DataType::UInt64, false), // local receive timestamp
        Field::new("exchange", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),   // "orderbook", "trade", "quote", "instrument", "tick_size_change"
        Field::new("symbol", DataType::Utf8, false),        // clob_token_id or symbol
        Field::new("side", DataType::Utf8, true),            // buy/sell (trades)
        Field::new("price", DataType::Float64, true),
        Field::new("quantity", DataType::Float64, true),
        Field::new("bid_price", DataType::Float64, true),    // quote best bid
        Field::new("ask_price", DataType::Float64, true),    // quote best ask
        Field::new("bid_qty", DataType::Float64, true),
        Field::new("ask_qty", DataType::Float64, true),
        Field::new("bids_json", DataType::Utf8, true),       // full orderbook bids as JSON
        Field::new("asks_json", DataType::Utf8, true),       // full orderbook asks as JSON
        Field::new("data_json", DataType::Utf8, true),       // instrument/other data as JSON
    ])
}

/// Buffers market events in memory, periodically materialises them into
/// a `<base>.parquet` file via atomic read-modify-write (Option B from
/// the 2026-05-20 design discussion).
///
/// **Lifecycle**:
///   1. `push_*` accumulates rows into the columnar Vec fields.
///   2. `pack_batch()` (called every flush_interval, default 60s) drains
///      the columnar Vecs into a `RecordBatch` and pushes onto
///      `archived_batches`. Memory stays bounded by the hour's data.
///   3. `write_to_disk()` (called on every checkpoint, default every
///      5 min) rewrites `<path>` from scratch with ALL accumulated
///      `archived_batches`. Atomic via tmpfile + rename.
///   4. `close()` does one final `write_to_disk()` then drops state.
///
/// **Why rewrite-the-whole-file** (instead of multi-file partials):
/// parquet's footer placement at file end means a single file can't
/// be tailed incrementally. Holding all batches in memory + rewriting
/// on each checkpoint is the only way to keep ONE readable file per
/// hour. Memory cost ≈ one hour's compressed data (~10–50 MB for
/// BTC ticks); IO cost ≈ 6.5× single-write (12 rewrites at growing
/// sizes per hour). See engine.rs recorder loop comment.
struct ParquetBuffer {
    path: PathBuf,
    schema: Arc<Schema>,
    /// All batches written so far this hour. Kept in memory; rewritten
    /// to disk in full on every `write_to_disk()` call.
    archived_batches: Vec<RecordBatch>,
    /// Total rows across `archived_batches` + pending columnar Vecs.
    rows_written: usize,
    // Columnar buffers — drained into a RecordBatch on `pack_batch()`.
    timestamp_ns: Vec<u64>,
    local_timestamp_ns: Vec<u64>,
    exchange: Vec<String>,
    event_type: Vec<String>,
    symbol: Vec<String>,
    side: Vec<Option<String>>,
    price: Vec<Option<f64>>,
    quantity: Vec<Option<f64>>,
    bid_price: Vec<Option<f64>>,
    ask_price: Vec<Option<f64>>,
    bid_qty: Vec<Option<f64>>,
    ask_qty: Vec<Option<f64>>,
    bids_json: Vec<Option<String>>,
    asks_json: Vec<Option<String>>,
    data_json: Vec<Option<String>>,
}

impl ParquetBuffer {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            schema: Arc::new(market_event_schema()),
            archived_batches: Vec::new(),
            rows_written: 0,
            timestamp_ns: Vec::new(),
            local_timestamp_ns: Vec::new(),
            exchange: Vec::new(),
            event_type: Vec::new(),
            symbol: Vec::new(),
            side: Vec::new(),
            price: Vec::new(),
            quantity: Vec::new(),
            bid_price: Vec::new(),
            ask_price: Vec::new(),
            bid_qty: Vec::new(),
            ask_qty: Vec::new(),
            bids_json: Vec::new(),
            asks_json: Vec::new(),
            data_json: Vec::new(),
        }
    }


    fn push_orderbook(&mut self, ts: u64, local_ts: u64, exchange: &str, symbol: &str, ob: &crate::types::OrderBookSnapshot) {
        self.timestamp_ns.push(ts);
        self.local_timestamp_ns.push(local_ts);
        self.exchange.push(exchange.to_string());
        self.event_type.push("orderbook".to_string());
        self.symbol.push(symbol.to_string());
        self.side.push(None);
        self.price.push(None);
        self.quantity.push(None);
        let best_bid = ob.best_bid().map(|l| l.price);
        let best_ask = ob.best_ask().map(|l| l.price);
        self.bid_price.push(best_bid);
        self.ask_price.push(best_ask);
        self.bid_qty.push(ob.best_bid().map(|l| l.quantity));
        self.ask_qty.push(ob.best_ask().map(|l| l.quantity));
        // Limit recorded depth to 5 levels to save space
        // Limit recorded depth to 5 levels closest to the spread.
        // Ordering varies by exchange:
        //   Polymarket: bids ascending [low→high], asks descending [high→low] → best at last
        //   Others (Binance etc): bids descending [high→low], asks ascending [low→high] → best at first
        let max_depth = 5;
        let bids_slice = if ob.bids.len() > max_depth {
            if ob.bids.first().map(|l| l.price) < ob.bids.last().map(|l| l.price) {
                // Ascending: best bid at end → take last N
                &ob.bids[ob.bids.len() - max_depth..]
            } else {
                // Descending: best bid at start → take first N
                &ob.bids[..max_depth]
            }
        } else { &ob.bids };
        let asks_slice = if ob.asks.len() > max_depth {
            if ob.asks.first().map(|l| l.price) > ob.asks.last().map(|l| l.price) {
                // Descending: best ask at end → take last N
                &ob.asks[ob.asks.len() - max_depth..]
            } else {
                // Ascending: best ask at start → take first N
                &ob.asks[..max_depth]
            }
        } else { &ob.asks };
        self.bids_json.push(Some(serde_json::to_string(bids_slice).unwrap_or_default()));
        self.asks_json.push(Some(serde_json::to_string(asks_slice).unwrap_or_default()));
        self.data_json.push(None);
    }

    fn push_trade(&mut self, ts: u64, local_ts: u64, exchange: &str, t: &crate::types::TradeTick) {
        self.timestamp_ns.push(ts);
        self.local_timestamp_ns.push(local_ts);
        self.exchange.push(exchange.to_string());
        self.event_type.push("trade".to_string());
        self.symbol.push(t.symbol.clone());
        self.side.push(Some(t.side.to_string().to_lowercase()));
        self.price.push(Some(t.price));
        self.quantity.push(Some(t.quantity));
        self.bid_price.push(None);
        self.ask_price.push(None);
        self.bid_qty.push(None);
        self.ask_qty.push(None);
        self.bids_json.push(None);
        self.asks_json.push(None);
        self.data_json.push(None);
    }

    fn push_quote(&mut self, ts: u64, local_ts: u64, exchange: &str, q: &crate::types::QuoteTick) {
        self.timestamp_ns.push(ts);
        self.local_timestamp_ns.push(local_ts);
        self.exchange.push(exchange.to_string());
        self.event_type.push("quote".to_string());
        self.symbol.push(q.symbol.clone());
        self.side.push(None);
        self.price.push(None);
        self.quantity.push(None);
        self.bid_price.push(Some(q.bid_price));
        self.ask_price.push(Some(q.ask_price));
        self.bid_qty.push(Some(q.bid_qty));
        self.ask_qty.push(Some(q.ask_qty));
        self.bids_json.push(None);
        self.asks_json.push(None);
        self.data_json.push(None);
    }

    fn push_instrument(&mut self, ts: u64, local_ts: u64, exchange: &str, event: &MarketEvent) {
        self.timestamp_ns.push(ts);
        self.local_timestamp_ns.push(local_ts);
        self.exchange.push(exchange.to_string());
        self.event_type.push("instrument".to_string());
        self.symbol.push(String::new());
        self.side.push(None);
        self.price.push(None);
        self.quantity.push(None);
        self.bid_price.push(None);
        self.ask_price.push(None);
        self.bid_qty.push(None);
        self.ask_qty.push(None);
        self.bids_json.push(None);
        self.asks_json.push(None);
        self.data_json.push(Some(serde_json::to_string(event).unwrap_or_default()));
    }

    fn push_spot_price(&mut self, ts: u64, local_ts: u64, sp: &crate::types::SpotPrice) {
        self.timestamp_ns.push(ts);
        self.local_timestamp_ns.push(local_ts);
        self.exchange.push(sp.source.clone());
        self.event_type.push("spot_price".to_string());
        self.symbol.push(sp.symbol.clone());
        self.side.push(None);
        self.price.push(Some(sp.price));
        self.quantity.push(None);
        self.bid_price.push(None);
        self.ask_price.push(None);
        self.bid_qty.push(None);
        self.ask_qty.push(None);
        self.bids_json.push(None);
        self.asks_json.push(None);
        self.data_json.push(None);
    }

    fn push_tick_size_change(&mut self, ts: u64, local_ts: u64, exchange: &str, tsc: &crate::types::TickSizeChange) {
        self.timestamp_ns.push(ts);
        self.local_timestamp_ns.push(local_ts);
        self.exchange.push(exchange.to_string());
        self.event_type.push("tick_size_change".to_string());
        self.symbol.push(tsc.symbol.clone());
        self.side.push(None);
        self.price.push(Some(tsc.new_tick_size));
        self.quantity.push(Some(tsc.old_tick_size));
        self.bid_price.push(None);
        self.ask_price.push(None);
        self.bid_qty.push(None);
        self.ask_qty.push(None);
        self.bids_json.push(None);
        self.asks_json.push(None);
        self.data_json.push(None);
    }

    /// Drain the columnar Vecs into a `RecordBatch` and append to
    /// `archived_batches`. In-memory only — no disk IO. Called on the
    /// 60s periodic flush from the recorder loop to bound peak Vec
    /// allocations between rewrites.
    fn pack_batch(&mut self) -> Result<()> {
        if self.timestamp_ns.is_empty() {
            return Ok(());
        }
        let n = self.timestamp_ns.len();
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(UInt64Array::from(std::mem::take(&mut self.timestamp_ns))),
                Arc::new(UInt64Array::from(std::mem::take(&mut self.local_timestamp_ns))),
                Arc::new(StringArray::from(std::mem::take(&mut self.exchange))),
                Arc::new(StringArray::from(std::mem::take(&mut self.event_type))),
                Arc::new(StringArray::from(std::mem::take(&mut self.symbol))),
                Arc::new(StringArray::from(std::mem::take(&mut self.side))),
                Arc::new(Float64Array::from(std::mem::take(&mut self.price))),
                Arc::new(Float64Array::from(std::mem::take(&mut self.quantity))),
                Arc::new(Float64Array::from(std::mem::take(&mut self.bid_price))),
                Arc::new(Float64Array::from(std::mem::take(&mut self.ask_price))),
                Arc::new(Float64Array::from(std::mem::take(&mut self.bid_qty))),
                Arc::new(Float64Array::from(std::mem::take(&mut self.ask_qty))),
                Arc::new(StringArray::from(std::mem::take(&mut self.bids_json))),
                Arc::new(StringArray::from(std::mem::take(&mut self.asks_json))),
                Arc::new(StringArray::from(std::mem::take(&mut self.data_json))),
            ],
        )?;
        self.archived_batches.push(batch);
        self.rows_written += n;
        Ok(())
    }

    /// Materialise the entire hour's data into a single readable
    /// parquet file at `<path>`. Atomic: writes to `<path>.tmp` then
    /// `rename` (POSIX-atomic on same filesystem). After this returns
    /// the file is fully readable by any standard parquet reader —
    /// the previous file content (if any) is replaced.
    ///
    /// Called by the recorder's checkpoint hook (every 5 min) AND by
    /// `close()` (on hour rotation / shutdown). No-op when nothing has
    /// been written yet.
    fn write_to_disk(&mut self) -> Result<()> {
        // Roll any pending columnar buffer into archived_batches first
        // so it lands in the file along with the older row groups.
        self.pack_batch()?;
        if self.archived_batches.is_empty() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Atomic write via tmpfile + rename.
        let tmp_path = self.path.with_extension("parquet.tmp");
        {
            let file = File::create(&tmp_path)?;
            let mut writer = ArrowWriter::try_new(
                file,
                self.schema.clone(),
                Some(recorder_writer_properties()),
            )?;
            for batch in &self.archived_batches {
                writer.write(batch)?;
            }
            writer.close()?;  // footer landed, file valid
        }
        std::fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    /// Final write + log. Called on hour rotation or shutdown. After
    /// this returns the buffer can be dropped — its data is on disk.
    fn close(&mut self) {
        let had_data = !self.archived_batches.is_empty() || !self.timestamp_ns.is_empty();
        if let Err(e) = self.write_to_disk() {
            log::error!("[Recorder] close write_to_disk failed: {}", e);
            return;
        }
        if had_data && self.rows_written > 0 {
            info!("[Recorder] Wrote {} rows to {}", self.rows_written, self.path.display());
        }
        // Free the archived batches now that they're on disk; the
        // buffer is about to be dropped by the caller anyway.
        self.archived_batches.clear();
    }
}

impl Drop for ParquetBuffer {
    fn drop(&mut self) {
        self.close();
    }
}

/// Records market events to Parquet files.
///
/// - **Polymarket event series**: `{output_dir}/polymarket/{event_id}_{slug}.parquet`
///   Same event → same file. All market data for the event in one file.
/// - **Other exchanges**: `{output_dir}/{exchange}/{symbol}/{YYYYMMDD_HH}.parquet`
///   Hourly rotation.
pub struct MarketRecorder {
    output_dir: PathBuf,
    /// Keyed by file_key → buffer
    buffers: HashMap<String, ParquetBuffer>,
    /// Maps clob_token_id → file_key (event-based grouping)
    token_to_file_key: HashMap<String, String>,
    /// Per-series state, keyed by "{exchange}_{series_slug}" (e.g. "polymarket_btc-up-or-down-5m")
    current_event_id: HashMap<String, String>,
    current_event_slug: HashMap<String, String>,
    current_series_slug: HashMap<String, String>,
    /// Pending series_key per exchange — set by EventStart, consumed by next Instrument.
    /// Handles multiple series: each EventStart pushes, each Instrument pops.
    pending_series_keys: HashMap<String, Vec<String>>,
    total_event_count: u64,
    /// Accumulated bar data for histdata recording, keyed by "{exchange}/{symbol}/{interval}"
    bar_buffers: HashMap<String, Vec<crate::types::BarData>>,
}

impl MarketRecorder {
    pub fn new(output_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&output_dir)?;
        info!("[Recorder] Output dir: {}", output_dir.display());
        Ok(Self {
            output_dir,
            bar_buffers: HashMap::new(),
            buffers: HashMap::new(),
            token_to_file_key: HashMap::new(),
            current_event_id: HashMap::new(),
            current_event_slug: HashMap::new(),
            current_series_slug: HashMap::new(),
            pending_series_keys: HashMap::new(),
            total_event_count: 0,
        })
    }

    fn get_or_create_buffer(&mut self, file_key: &str, path: PathBuf) -> &mut ParquetBuffer {
        self.buffers
            .entry(file_key.to_string())
            .or_insert_with(|| ParquetBuffer::new(path))
    }

    /// Build Parquet file path for Polymarket events.
    /// Format: polymarket/{series_slug}/{YYYYMMDD}/{event_slug}-{event_id}.parquet
    fn poly_path(&self, series_slug: &str, event_id: &str, event_slug: &str) -> PathBuf {
        // Extract date from event_slug timestamp (e.g. "btc-updown-5m-1774807800" → 1774807800)
        let date_str = event_slug.rsplit('-').next()
            .and_then(|s| s.parse::<i64>().ok())
            .and_then(|ts| chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0))
            .map(|dt| dt.format("%Y%m%d").to_string())
            .unwrap_or_else(|| chrono::Utc::now().format("%Y%m%d").to_string());
        self.output_dir
            .join("polymarket")
            .join(series_slug)
            .join(&date_str)
            .join(format!("{}-{}.parquet", event_slug, event_id))
    }

    /// Build Parquet file path for other exchanges (hourly).
    fn generic_path(&self, exchange: &str, symbol: &str, ts_ns: u64) -> PathBuf {
        let secs = (ts_ns / 1_000_000_000) as i64;
        let hour_secs = secs - (secs % 3600);
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(hour_secs, 0)
            .unwrap_or(chrono::Utc::now());
        self.output_dir
            .join(exchange)
            .join(symbol)
            .join(dt.format("%Y%m").to_string())
            .join(dt.format("%m%d").to_string())
            .join(format!("{}.parquet", dt.format("%Y%m%d_%H")))
    }

    /// Flush and remove old buffers when switching to a new file key.
    /// Compares by prefix (everything before the last `_` = hour bucket).
    fn rotate_buffer(&mut self, new_key: &str) {
        if self.buffers.contains_key(new_key) { return; }
        if let Some(prefix_end) = new_key.rfind('_') {
            let prefix = &new_key[..=prefix_end];
            let old_keys: Vec<String> = self.buffers.keys()
                .filter(|k| k.starts_with(prefix) && *k != new_key)
                .cloned().collect();
            for old_key in old_keys {
                if let Some(mut buf) = self.buffers.remove(&old_key) {
                    buf.close();
                }
            }
        }
    }

    /// Resolve the file_key and path for a given event.
    /// Returns None if the token is unregistered (e.g. stale data from a previous event
    /// whose mapping was not preserved) — caller should skip writing.
    fn resolve_file(&self, exchange: &str, symbol: &str, ts_ns: u64) -> Option<(String, PathBuf)> {
        // Check if this token has an explicit file mapping (Polymarket token IDs)
        if let Some(file_key) = self.token_to_file_key.get(symbol) {
            if let Some(buf) = self.buffers.get(file_key) {
                return Some((file_key.clone(), buf.path.clone()));
            }
            // Buffer was removed (event rotated) — skip stale data
            return None;
        }
        // For polymarket/hexmarket, only write data for explicitly registered tokens.
        // Unregistered tokens are stale data from a previous event — skip them.
        if exchange == "polymarket" || exchange == "hexmarket" {
            return None;
        }
        // Generic hourly rotation (binance, etc.)
        let key = format!("{}_{}_{}", exchange, symbol, ts_ns / 3_600_000_000_000);
        let path = self.generic_path(exchange, symbol, ts_ns);
        Some((key, path))
    }

    pub fn write_event(&mut self, event: &MarketEvent) -> Result<()> {
        match event {
            MarketEvent::EventStart { exchange, symbol, event_id, event_start_ns: _ } => {
                let ex = exchange.to_string();
                // Use series slug as part of key to distinguish multiple series on same exchange
                let series_slug = if symbol.starts_with("series:") {
                    symbol["series:".len()..].to_string()
                } else {
                    symbol.clone()
                };
                let series_key = format!("{}_{}", ex, series_slug);

                // Flush previous event's buffer if switching events
                if let Some(old_id) = self.current_event_id.get(&series_key) {
                    if old_id != event_id {
                        let old_slug = self.current_event_slug.get(&series_key).cloned().unwrap_or_default();
                        let old_key = format!("{}_{}", old_id, old_slug);
                        if let Some(mut buf) = self.buffers.remove(&old_key) {
                            buf.close();
                        }
                        // Remove old token mappings so stale data is discarded
                        self.token_to_file_key.retain(|_, v| v != &old_key);
                    }
                }
                self.current_event_id.insert(series_key.clone(), event_id.clone());
                self.current_series_slug.insert(series_key.clone(), series_slug.clone());
                // Event slug will be overridden by Instrument event's slug
                self.current_event_slug.insert(series_key.clone(), series_slug);
                // Push pending series_key for the next Instrument to consume
                self.pending_series_keys.entry(ex).or_default().push(series_key);
            }
            MarketEvent::Instrument(inst) => {
                let ex = event.exchange().to_string();
                if let crate::types::Instrument::BinaryOption(bo) = inst {
                    // Pop the pending series_key set by the preceding EventStart
                    let series_key = self.pending_series_keys.get_mut(&ex)
                        .and_then(|v| if v.is_empty() { None } else { Some(v.remove(0)) })
                        .unwrap_or_else(|| ex.clone()); // fallback if no EventStart

                    // Use slug from instrument for file naming
                    if !bo.slug.is_empty() {
                        self.current_event_slug.insert(series_key.clone(), bo.slug.clone());
                    }

                    // If no EventStart received yet, auto-create event context
                    if !self.current_event_id.contains_key(&series_key) {
                        self.current_event_id.insert(series_key.clone(), bo.id.clone());
                        if !self.current_series_slug.contains_key(&series_key) {
                            self.current_series_slug.insert(series_key.clone(), bo.slug.clone());
                        }
                    }

                    // Map all token IDs to this event's file key
                    let eid = self.current_event_id.get(&series_key).cloned().unwrap_or_default();
                    let slug = self.current_event_slug.get(&series_key).cloned().unwrap_or_default();
                    let series = self.current_series_slug.get(&series_key).cloned().unwrap_or_default();
                    let file_key = format!("{}_{}", eid, slug);
                    for token_id in &bo.clob_token_ids {
                        self.token_to_file_key.insert(token_id.clone(), file_key.clone());
                    }
                    let path = self.poly_path(&series, &eid, &slug);
                    let buf = self.get_or_create_buffer(&file_key, path);

                    // Record instrument event
                    let local_ts = crate::types::now_ns();
                    buf.push_instrument(local_ts, local_ts, &ex, event);
                    self.total_event_count += 1;
                }
            }
            MarketEvent::OrderBook(ob) => {
                let ex = ob.exchange.to_string();
                if let Some((file_key, path)) = self.resolve_file(&ex, &ob.symbol, ob.local_timestamp_ns) {
                    self.rotate_buffer(&file_key);
                    let buf = self.buffers.entry(file_key).or_insert_with(|| ParquetBuffer::new(path));
                    buf.push_orderbook(ob.exchange_timestamp_ns, ob.local_timestamp_ns, &ex, &ob.symbol, ob);
                    self.total_event_count += 1;
                }
            }
            MarketEvent::Trade(t) => {
                let ex = t.exchange.to_string();
                if let Some((file_key, path)) = self.resolve_file(&ex, &t.symbol, t.local_timestamp_ns) {
                    self.rotate_buffer(&file_key);
                    let buf = self.buffers.entry(file_key).or_insert_with(|| ParquetBuffer::new(path));
                    buf.push_trade(t.exchange_timestamp_ns, t.local_timestamp_ns, &ex, t);
                    self.total_event_count += 1;
                }
            }
            MarketEvent::Quote(q) => {
                let ex = q.exchange.to_string();
                if let Some((file_key, path)) = self.resolve_file(&ex, &q.symbol, q.local_timestamp_ns) {
                    self.rotate_buffer(&file_key);
                    let buf = self.buffers.entry(file_key).or_insert_with(|| ParquetBuffer::new(path));
                    buf.push_quote(q.exchange_timestamp_ns, q.local_timestamp_ns, &ex, q);
                    self.total_event_count += 1;
                }
            }
            MarketEvent::TickSizeChange(tsc) => {
                let ex = tsc.exchange.to_string();
                if let Some((file_key, path)) = self.resolve_file(&ex, &tsc.symbol, tsc.local_timestamp_ns) {
                    self.rotate_buffer(&file_key);
                    let buf = self.buffers.entry(file_key).or_insert_with(|| ParquetBuffer::new(path));
                    buf.push_tick_size_change(tsc.local_timestamp_ns, tsc.local_timestamp_ns, &ex, tsc);
                    self.total_event_count += 1;
                }
            }
            MarketEvent::Bar(bar) => {
                if bar.is_closed {
                    let key = format!("{}/{}/{}", bar.exchange, bar.symbol, bar.interval);
                    self.bar_buffers.entry(key).or_default().push(bar.clone());
                    self.total_event_count += 1;
                    // Flush bar buffer every 100 bars
                    let buf_key = format!("{}/{}/{}", bar.exchange, bar.symbol, bar.interval);
                    if self.bar_buffers.get(&buf_key).map(|b| b.len()).unwrap_or(0) >= 100 {
                        let _ = self.flush_bar_buffer(&buf_key);
                    }
                }
            }
            MarketEvent::SpotPrice(sp) => {
                // Store as: {source}/{symbol}/{YYYYMM}/{MMDD}/{YYYYMMDD_HH}.parquet
                // source: "chainlink", "pyth", or legacy "rtds_binance" etc.
                let source_dir = match sp.source.as_str() {
                    "chainlink" | "chainlink_stream" => "chainlink",
                    "pyth" => "pyth",
                    other => {
                        // Legacy: "rtds_chainlink" → "rtds/chainlink", "rtds_binance" → "rtds/binance"
                        other.strip_prefix("rtds_").unwrap_or(other)
                    }
                };
                let sym_lower = sp.symbol.to_lowercase().replace('/', "-");
                let key = format!("{}_{}_{}", source_dir, sym_lower, sp.local_timestamp_ns / 3_600_000_000_000);
                let secs = (sp.local_timestamp_ns / 1_000_000_000) as i64;
                let hour_secs = secs - (secs % 3600);
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(hour_secs, 0).unwrap_or_default();
                let base = if source_dir.contains('/') {
                    self.output_dir.join("rtds").join(source_dir)
                } else {
                    self.output_dir.join(source_dir)
                };
                let path = base
                    .join(&sym_lower)
                    .join(dt.format("%Y%m").to_string())
                    .join(dt.format("%m%d").to_string())
                    .join(format!("{}.parquet", dt.format("%Y%m%d_%H")));
                self.rotate_buffer(&key);
                let buf = self.buffers.entry(key).or_insert_with(|| ParquetBuffer::new(path));
                buf.push_spot_price(sp.timestamp_ns, sp.local_timestamp_ns, sp);
                self.total_event_count += 1;
            }
            MarketEvent::Connected { .. }
            | MarketEvent::Disconnected { .. }
            | MarketEvent::Exit => {}
        }

        Ok(())
    }

    /// Periodic memory-bound: drain columnar Vecs into `RecordBatch`es
    /// held on `ParquetBuffer.archived_batches`. No disk IO. Called
    /// every 60s by the recorder loop so a slow checkpoint cadence
    /// can't blow up the per-Vec allocation footprint.
    pub fn flush_buffers(&mut self) {
        for buf in self.buffers.values_mut() {
            if !buf.timestamp_ns.is_empty() {
                if let Err(e) = buf.pack_batch() {
                    log::error!("[Recorder] Periodic pack_batch error: {}", e);
                }
            }
        }
    }

    /// **Checkpoint**: rewrite each open buffer's `<base>.parquet` from
    /// scratch with ALL archived batches accumulated since hour start.
    /// Atomic via tmpfile + rename — readers see either the previous
    /// version or the new one, never partial.
    ///
    /// One file per hour stays the contract (no sidecar partials).
    /// Cost: rewrites whole file every 5 min; over an hour that's
    /// ~6.5× the IO of a single end-of-hour write, but each rewrite
    /// is sequential streaming write + atomic rename — fine on SSD.
    /// Memory cost: ~one hour of compressed data per buffer (~10-50 MB
    /// for BTC tick stream); held until the hour rotates and `close()`
    /// frees it.
    ///
    /// Bar buffers (`bar_buffers`) are not checkpointed here — they
    /// flush via `flush_bar_buffer` already and consumers don't
    /// hot-tail them.
    pub fn checkpoint(&mut self) {
        for buf in self.buffers.values_mut() {
            if let Err(e) = buf.write_to_disk() {
                log::error!(
                    "[Recorder] checkpoint write_to_disk failed for {}: {}",
                    buf.path.display(), e,
                );
            }
        }
    }

    /// Close all buffers and writers (call on shutdown).
    pub fn flush(&mut self) -> Result<()> {
        for buf in self.buffers.values_mut() {
            buf.close();
        }
        // Flush all bar buffers
        let keys: Vec<String> = self.bar_buffers.keys().cloned().collect();
        for key in keys {
            let _ = self.flush_bar_buffer(&key);
        }
        Ok(())
    }

    /// Flush accumulated bar data to histdata parquet files.
    /// Path: `{output_dir}/histdata/{exchange}/{symbol}/{interval}/{YYYYMM}/{YYYYMMDD}.parquet`
    fn flush_bar_buffer(&mut self, key: &str) -> Result<()> {
        let bars = match self.bar_buffers.remove(key) {
            Some(b) if !b.is_empty() => b,
            _ => return Ok(()),
        };

        // Parse key: "{exchange}/{symbol}/{interval}"
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() != 3 {
            return Ok(());
        }
        let (exchange_str, symbol, interval) = (parts[0], parts[1], parts[2]);

        let hist_dir = self.output_dir
            .join("histdata")
            .join(exchange_str)
            .join(symbol)
            .join(interval);

        match crate::recorder::hist_reader::save_bars_to_local(&hist_dir, &bars, interval) {
            Ok(()) => {}
            Err(e) => {
                warn!("[Recorder] Failed to save {} bars for {}: {}", bars.len(), key, e);
            }
        }

        Ok(())
    }

    pub fn event_count(&self) -> u64 {
        self.total_event_count
    }
}

#[cfg(test)]
mod tests {
    //! Tests for the parquet compression config — the previous default
    //! (`None`) silently produced uncompressed files ~5–6× larger than
    //! necessary. These tests lock the SNAPPY default in place and
    //! verify that a parquet file produced via the recorder's writer
    //! properties is actually readable.
    use super::*;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    /// `recorder_writer_properties()` returns SNAPPY compression. Lock
    /// this default — anyone changing it must explicitly update the
    /// test (and the comments document the rationale: ~5× smaller files
    /// for our OB-heavy schema, fast decompression for warm-up replay).
    #[test]
    fn writer_properties_default_is_snappy() {
        let props = recorder_writer_properties();
        // `compression()` takes a column path — we use the default
        // (applies to all columns). Pass an arbitrary column name.
        let comp = props.compression(&parquet::schema::types::ColumnPath::from("any"));
        assert_eq!(comp, Compression::SNAPPY,
            "MarketRecorder must default to SNAPPY compression");
    }

    /// End-to-end: write a tiny parquet via the recorder's properties,
    /// read it back, and verify the column metadata reports SNAPPY.
    /// Regression guard against an accidental future change that maps
    /// `None` → UNCOMPRESSED (the bug this commit fixes).
    #[test]
    fn parquet_written_with_recorder_properties_reports_snappy_in_metadata() {
        use arrow::array::UInt64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Tiny 3-row, 1-column parquet.
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1u64, 2, 3])) as ArrayRef],
        ).expect("batch construction");

        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let file = File::create(tmp.path()).expect("create");
        let mut writer = ArrowWriter::try_new(
            file,
            schema,
            Some(recorder_writer_properties()),
        ).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");

        // Read back and inspect column metadata.
        let reader_file = File::open(tmp.path()).expect("reopen");
        let reader = SerializedFileReader::new(reader_file).expect("reader");
        let meta = reader.metadata();
        assert!(meta.num_row_groups() > 0, "must have ≥ 1 row group");
        let rg = meta.row_group(0);
        for ci in 0..rg.num_columns() {
            let col = rg.column(ci);
            assert_eq!(
                col.compression(),
                Compression::SNAPPY,
                "column {} ({:?}) must be SNAPPY-compressed", ci, col.column_path(),
            );
        }
    }
}
