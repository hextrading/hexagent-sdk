use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use log::{info, warn};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// One bounded in-memory row group per open recorder stream.  At the old
/// 60-second pack cadence a busy book stream could retain an hour of Arrow
/// arrays; 8,192 rows keeps the live working set predictable and is still
/// large enough for efficient Parquet compression.
const RECORDER_ROW_GROUP_ROWS: usize = 8_192;

static RECORDER_BUFFERED_ROWS: AtomicU64 = AtomicU64::new(0);
static RECORDER_BUFFERED_BYTES: AtomicU64 = AtomicU64::new(0);
static RECORDER_WRITTEN_ROWS: AtomicU64 = AtomicU64::new(0);
static RECORDER_WRITTEN_BYTES: AtomicU64 = AtomicU64::new(0);
static RECORDER_OPEN_STREAMS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecorderStats {
    pub buffered_rows: u64,
    pub buffered_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub open_streams: u64,
}

pub fn recorder_stats() -> RecorderStats {
    RecorderStats {
        buffered_rows: RECORDER_BUFFERED_ROWS.load(Ordering::Acquire),
        buffered_bytes: RECORDER_BUFFERED_BYTES.load(Ordering::Acquire),
        written_rows: RECORDER_WRITTEN_ROWS.load(Ordering::Acquire),
        written_bytes: RECORDER_WRITTEN_BYTES.load(Ordering::Acquire),
        open_streams: RECORDER_OPEN_STREAMS.load(Ordering::Acquire),
    }
}

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
        Field::new("timestamp_ns", DataType::UInt64, false), // exchange timestamp
        Field::new("local_timestamp_ns", DataType::UInt64, false), // local receive timestamp
        Field::new("exchange", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false), // "orderbook", "trade", "quote", "instrument", "market_data_health", "tick_size_change"
        Field::new("symbol", DataType::Utf8, false),     // clob_token_id or symbol
        Field::new("side", DataType::Utf8, true),        // buy/sell (trades)
        Field::new("price", DataType::Float64, true),
        Field::new("quantity", DataType::Float64, true),
        Field::new("bid_price", DataType::Float64, true), // quote best bid
        Field::new("ask_price", DataType::Float64, true), // quote best ask
        Field::new("bid_qty", DataType::Float64, true),
        Field::new("ask_qty", DataType::Float64, true),
        Field::new("bids_json", DataType::Utf8, true), // full orderbook bids as JSON
        Field::new("asks_json", DataType::Utf8, true), // full orderbook asks as JSON
        Field::new("data_json", DataType::Utf8, true), // instrument/other data as JSON
    ])
}

/// Buffers one fixed-capacity row group and writes immutable Parquet shards.
///
/// **Lifecycle**:
///   1. `push_*` accumulates rows into the columnar Vec fields.
///   2. Reaching [`RECORDER_ROW_GROUP_ROWS`] synchronously closes one
///      `<base>.part-NNNNNN.parquet` shard on the recorder worker.
///   3. A checkpoint flushes the partial group and immediately releases its
///      Arrow arrays. Completed shards are never retained or rewritten.
///   4. `close()` flushes the final partial group.
struct ParquetBuffer {
    path: PathBuf,
    schema: Arc<Schema>,
    next_shard_id: u64,
    /// At most one encoded row group is retained after a disk error so the
    /// recorder can retry without dropping private/public market evidence.
    pending_batch: Option<RecordBatch>,
    /// Total rows durably written across completed shards.
    rows_written: usize,
    // One fixed-capacity columnar row group.
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
            next_shard_id: 0,
            pending_batch: None,
            rows_written: 0,
            timestamp_ns: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            local_timestamp_ns: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            exchange: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            event_type: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            symbol: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            side: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            price: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            quantity: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            bid_price: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            ask_price: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            bid_qty: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            ask_qty: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            bids_json: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            asks_json: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
            data_json: Vec::with_capacity(RECORDER_ROW_GROUP_ROWS),
        }
    }

    fn push_orderbook(
        &mut self,
        ts: u64,
        local_ts: u64,
        exchange: &str,
        symbol: &str,
        ob: &crate::types::OrderBookSnapshot,
    ) {
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
        } else {
            &ob.bids
        };
        let asks_slice = if ob.asks.len() > max_depth {
            if ob.asks.first().map(|l| l.price) > ob.asks.last().map(|l| l.price) {
                // Descending: best ask at end → take last N
                &ob.asks[ob.asks.len() - max_depth..]
            } else {
                // Ascending: best ask at start → take first N
                &ob.asks[..max_depth]
            }
        } else {
            &ob.asks
        };
        self.bids_json
            .push(Some(serde_json::to_string(bids_slice).unwrap_or_default()));
        self.asks_json
            .push(Some(serde_json::to_string(asks_slice).unwrap_or_default()));
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
        self.data_json
            .push(Some(serde_json::to_string(event).unwrap_or_default()));
    }

    fn push_market_data_health(
        &mut self,
        local_ts: u64,
        exchange: &str,
        symbol: &str,
        event: &MarketEvent,
    ) {
        self.timestamp_ns.push(local_ts);
        self.local_timestamp_ns.push(local_ts);
        self.exchange.push(exchange.to_string());
        self.event_type.push("market_data_health".to_string());
        self.symbol.push(symbol.to_string());
        self.side.push(None);
        self.price.push(None);
        self.quantity.push(None);
        self.bid_price.push(None);
        self.ask_price.push(None);
        self.bid_qty.push(None);
        self.ask_qty.push(None);
        self.bids_json.push(None);
        self.asks_json.push(None);
        self.data_json
            .push(Some(serde_json::to_string(event).unwrap_or_default()));
    }

    /// Asset-context row (`event_type = "asset_ctx"`): mark px in `price`,
    /// impact bid/ask in `bid_price`/`ask_price`, remaining ctx fields as
    /// compact JSON in `data_json`.
    fn push_asset_ctx(
        &mut self,
        ts: u64,
        local_ts: u64,
        exchange: &str,
        ac: &crate::types::AssetCtxTick,
    ) {
        self.timestamp_ns.push(ts);
        self.local_timestamp_ns.push(local_ts);
        self.exchange.push(exchange.to_string());
        self.event_type.push("asset_ctx".to_string());
        self.symbol.push(ac.symbol.clone());
        self.side.push(None);
        self.price.push(Some(ac.mark_px));
        self.quantity.push(None);
        self.bid_price.push(Some(ac.impact_bid_px));
        self.ask_price.push(Some(ac.impact_ask_px));
        self.bid_qty.push(None);
        self.ask_qty.push(None);
        self.bids_json.push(None);
        self.asks_json.push(None);
        self.data_json.push(Some(format!(
            "{{\"oraclePx\":{},\"midPx\":{},\"funding\":{},\"openInterest\":{},\"premium\":{},\"dayNtlVlm\":{},\"prevDayPx\":{}}}",
            ac.oracle_px, ac.mid_px, ac.funding, ac.open_interest, ac.premium, ac.day_ntl_vlm, ac.prev_day_px,
        )));
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

    fn push_tick_size_change(
        &mut self,
        ts: u64,
        local_ts: u64,
        exchange: &str,
        tsc: &crate::types::TickSizeChange,
    ) {
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

    fn replace_column<T>(column: &mut Vec<T>) -> Vec<T> {
        std::mem::replace(column, Vec::with_capacity(RECORDER_ROW_GROUP_ROWS))
    }

    /// Drain the active fixed-capacity columns into one Arrow batch. A fresh
    /// bounded set of columns is installed before constructing the batch, so
    /// the stream can never retain more than one raw row group.
    fn take_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.timestamp_ns.is_empty() {
            return Ok(None);
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(UInt64Array::from(Self::replace_column(
                    &mut self.timestamp_ns,
                ))),
                Arc::new(UInt64Array::from(Self::replace_column(
                    &mut self.local_timestamp_ns,
                ))),
                Arc::new(StringArray::from(Self::replace_column(&mut self.exchange))),
                Arc::new(StringArray::from(Self::replace_column(
                    &mut self.event_type,
                ))),
                Arc::new(StringArray::from(Self::replace_column(&mut self.symbol))),
                Arc::new(StringArray::from(Self::replace_column(&mut self.side))),
                Arc::new(Float64Array::from(Self::replace_column(&mut self.price))),
                Arc::new(Float64Array::from(Self::replace_column(&mut self.quantity))),
                Arc::new(Float64Array::from(Self::replace_column(
                    &mut self.bid_price,
                ))),
                Arc::new(Float64Array::from(Self::replace_column(
                    &mut self.ask_price,
                ))),
                Arc::new(Float64Array::from(Self::replace_column(&mut self.bid_qty))),
                Arc::new(Float64Array::from(Self::replace_column(&mut self.ask_qty))),
                Arc::new(StringArray::from(Self::replace_column(&mut self.bids_json))),
                Arc::new(StringArray::from(Self::replace_column(&mut self.asks_json))),
                Arc::new(StringArray::from(Self::replace_column(&mut self.data_json))),
            ],
        )?;
        Ok(Some(batch))
    }

    fn shard_path(&self, shard_id: u64) -> PathBuf {
        let stem = self
            .path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("market");
        self.path
            .with_file_name(format!("{stem}.part-{shard_id:06}.parquet"))
    }

    /// Write and close exactly one immutable Parquet shard, then drop its
    /// Arrow batch. Existing shard names are skipped so a process restart in
    /// the same hour appends without overwriting earlier checkpoints.
    fn write_pending_shards(&mut self) -> Result<()> {
        loop {
            if self.pending_batch.is_none() {
                self.pending_batch = self.take_batch()?;
            }
            let Some(batch) = self.pending_batch.as_ref() else {
                return Ok(());
            };
            let row_count = batch.num_rows();
            if let Some(parent) = self.path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let shard_path = loop {
                let candidate = self.shard_path(self.next_shard_id);
                self.next_shard_id = self.next_shard_id.saturating_add(1);
                if !candidate.exists() {
                    break candidate;
                }
            };
            let tmp_path = shard_path.with_extension("parquet.tmp");
            {
                let file = File::create(&tmp_path)?;
                let mut writer = ArrowWriter::try_new(
                    file,
                    self.schema.clone(),
                    Some(recorder_writer_properties()),
                )?;
                writer.write(batch)?;
                writer.close()?;
            }
            std::fs::rename(&tmp_path, &shard_path)?;
            let bytes = std::fs::metadata(&shard_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            self.pending_batch = None;
            self.rows_written = self.rows_written.saturating_add(row_count);
            RECORDER_WRITTEN_ROWS.fetch_add(row_count as u64, Ordering::AcqRel);
            RECORDER_WRITTEN_BYTES.fetch_add(bytes, Ordering::AcqRel);
        }
    }

    fn flush_if_full(&mut self) -> Result<()> {
        if self.timestamp_ns.len() >= RECORDER_ROW_GROUP_ROWS {
            self.write_pending_shards()?;
        }
        Ok(())
    }

    fn prepare_for_row(&mut self) -> Result<()> {
        if self.timestamp_ns.len() >= RECORDER_ROW_GROUP_ROWS {
            self.write_pending_shards()?;
        }
        Ok(())
    }

    fn buffered_rows(&self) -> usize {
        self.timestamp_ns.len()
            + self
                .pending_batch
                .as_ref()
                .map(RecordBatch::num_rows)
                .unwrap_or(0)
    }

    fn buffered_bytes(&self) -> usize {
        let string_bytes = self.exchange.iter().map(String::len).sum::<usize>()
            + self.event_type.iter().map(String::len).sum::<usize>()
            + self.symbol.iter().map(String::len).sum::<usize>()
            + self
                .side
                .iter()
                .filter_map(Option::as_ref)
                .map(String::len)
                .sum::<usize>()
            + self
                .bids_json
                .iter()
                .filter_map(Option::as_ref)
                .map(String::len)
                .sum::<usize>()
            + self
                .asks_json
                .iter()
                .filter_map(Option::as_ref)
                .map(String::len)
                .sum::<usize>()
            + self
                .data_json
                .iter()
                .filter_map(Option::as_ref)
                .map(String::len)
                .sum::<usize>();
        let fixed_bytes = self.timestamp_ns.len()
            * (std::mem::size_of::<u64>() * 2 + std::mem::size_of::<Option<f64>>() * 8);
        let pending_bytes = self
            .pending_batch
            .as_ref()
            .map(|batch| {
                batch
                    .columns()
                    .iter()
                    .map(|array| array.get_array_memory_size())
                    .sum::<usize>()
            })
            .unwrap_or(0);
        string_bytes
            .saturating_add(fixed_bytes)
            .saturating_add(pending_bytes)
    }

    /// Final write + log. Called on hour rotation or shutdown. After
    /// this returns the buffer can be dropped — its data is on disk.
    fn close(&mut self) {
        let had_data = !self.timestamp_ns.is_empty();
        if let Err(e) = self.write_pending_shards() {
            log::error!("[Recorder] close shard write failed: {}", e);
            return;
        }
        if (had_data || self.rows_written > 0) && self.rows_written > 0 {
            info!(
                "[Recorder] Wrote {} rows as bounded shards for {}",
                self.rows_written,
                self.path.display()
            );
        }
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
    /// Exact `(exchange,event_id) -> series_key` lifecycle routing. Matching by
    /// event id prevents a delayed Instrument from consuming the next series'
    /// FIFO slot.
    pending_event_series: HashMap<String, String>,
    event_to_series: HashMap<String, String>,
    /// Bounded tombstones reject delayed Instruments after EventEnd/rotation.
    retired_events: HashSet<String>,
    retired_event_order: VecDeque<String>,
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
            pending_event_series: HashMap::new(),
            event_to_series: HashMap::new(),
            retired_events: HashSet::new(),
            retired_event_order: VecDeque::new(),
            total_event_count: 0,
        })
    }

    fn get_or_create_buffer(&mut self, file_key: &str, path: PathBuf) -> &mut ParquetBuffer {
        self.buffers
            .entry(file_key.to_string())
            .or_insert_with(|| ParquetBuffer::new(path))
    }

    fn publish_stats(&self) {
        let buffered_rows = self
            .buffers
            .values()
            .map(ParquetBuffer::buffered_rows)
            .sum::<usize>()
            .saturating_add(self.bar_buffers.values().map(Vec::len).sum::<usize>());
        let buffered_bytes = self
            .buffers
            .values()
            .map(ParquetBuffer::buffered_bytes)
            .sum::<usize>()
            .saturating_add(
                self.bar_buffers
                    .values()
                    .map(|bars| {
                        bars.len()
                            .saturating_mul(std::mem::size_of::<crate::types::BarData>())
                    })
                    .sum::<usize>(),
            );
        RECORDER_BUFFERED_ROWS.store(buffered_rows as u64, Ordering::Release);
        RECORDER_BUFFERED_BYTES.store(buffered_bytes as u64, Ordering::Release);
        RECORDER_OPEN_STREAMS.store(self.buffers.len() as u64, Ordering::Release);
    }

    fn series_slug(symbol: &str) -> &str {
        symbol.strip_prefix("series:").unwrap_or(symbol)
    }

    fn event_key(exchange: &str, event_id: &str) -> String {
        format!("{exchange}\u{1f}{event_id}")
    }

    fn remember_retired(&mut self, event_key: String) {
        const RETIRED_EVENT_CAPACITY: usize = 4_096;
        if self.retired_events.insert(event_key.clone()) {
            self.retired_event_order.push_back(event_key);
        }
        while self.retired_event_order.len() > RETIRED_EVENT_CAPACITY {
            if let Some(expired) = self.retired_event_order.pop_front() {
                self.retired_events.remove(&expired);
            }
        }
    }

    fn retire_event_context(&mut self, exchange: &str, event_id: &str, retired_symbols: &[String]) {
        let event_key = Self::event_key(exchange, event_id);
        self.pending_event_series.remove(&event_key);
        let series_key = self.event_to_series.remove(&event_key).or_else(|| {
            self.current_event_id
                .iter()
                .find_map(|(series, current)| (current == event_id).then(|| series.clone()))
        });
        if let Some(series_key) = series_key {
            let slug = self
                .current_event_slug
                .get(&series_key)
                .cloned()
                .unwrap_or_default();
            let file_key = format!("{event_id}_{slug}");
            if let Some(mut buffer) = self.buffers.remove(&file_key) {
                buffer.close();
            }
            self.token_to_file_key.retain(|symbol, mapped| {
                mapped != &file_key && !retired_symbols.iter().any(|retired| retired == symbol)
            });
            if self.current_event_id.get(&series_key).map(String::as_str) == Some(event_id) {
                self.current_event_id.remove(&series_key);
                self.current_event_slug.remove(&series_key);
                self.current_series_slug.remove(&series_key);
            }
        } else {
            self.token_to_file_key
                .retain(|symbol, _| !retired_symbols.iter().any(|retired| retired == symbol));
        }
        self.remember_retired(event_key);
    }

    fn fallback_series_slug(event_slug: &str) -> String {
        event_slug
            .rsplit_once('-')
            .filter(|(_, suffix)| suffix.parse::<u64>().is_ok())
            .map(|(prefix, _)| prefix.to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "fallback".to_string())
    }

    /// Build Parquet file path for Polymarket events.
    /// Format: polymarket/{series_slug}/{YYYYMMDD}/{event_slug}-{event_id}.parquet
    fn poly_path(&self, series_slug: &str, event_id: &str, event_slug: &str) -> PathBuf {
        // Extract date from event_slug timestamp (e.g. "btc-updown-5m-1774807800" → 1774807800)
        let date_str = event_slug
            .rsplit('-')
            .next()
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
        if self.buffers.contains_key(new_key) {
            return;
        }
        if let Some(prefix_end) = new_key.rfind('_') {
            let prefix = &new_key[..=prefix_end];
            let old_keys: Vec<String> = self
                .buffers
                .keys()
                .filter(|k| k.starts_with(prefix) && *k != new_key)
                .cloned()
                .collect();
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
            MarketEvent::EventStart {
                exchange,
                symbol,
                event_id,
                event_start_ns: _,
            } => {
                let ex = exchange.to_string();
                let series_slug = Self::series_slug(symbol).to_string();
                let canonical_series_key = format!("{}_{}", ex, series_slug);
                let event_key = Self::event_key(&ex, event_id);
                // If an Instrument legitimately arrived before EventStart,
                // keep its event-specific fallback context rather than
                // opening a second file for the same event.
                let instrument_preceded_start = self.event_to_series.contains_key(&event_key);
                let series_key = self
                    .event_to_series
                    .get(&event_key)
                    .cloned()
                    .unwrap_or(canonical_series_key);

                if let Some(old_id) = self.current_event_id.get(&series_key).cloned() {
                    if old_id.as_str() != event_id.as_str() {
                        self.retire_event_context(&ex, &old_id, &[]);
                    }
                }
                self.current_event_id
                    .insert(series_key.clone(), event_id.clone());
                self.current_series_slug
                    .insert(series_key.clone(), series_slug.clone());
                // Event slug will be overridden by a later Instrument. If the
                // Instrument arrived first, preserve its full rotating slug so
                // EventEnd closes the already-open fallback buffer.
                if !instrument_preceded_start {
                    self.current_event_slug
                        .insert(series_key.clone(), series_slug);
                }
                self.event_to_series
                    .insert(event_key.clone(), series_key.clone());
                self.pending_event_series.insert(event_key, series_key);
            }
            MarketEvent::Instrument(inst) => {
                let ex = event.exchange().to_string();
                if let crate::types::Instrument::BinaryOption(bo) = inst {
                    let event_key = Self::event_key(&ex, &bo.id);
                    if self.retired_events.contains(&event_key) {
                        warn!(
                            "[Recorder] Ignoring stale Instrument after retirement exchange={} event_id={} slug={}",
                            ex, bo.id, bo.slug,
                        );
                        self.publish_stats();
                        return Ok(());
                    }
                    let series_key = self
                        .pending_event_series
                        .remove(&event_key)
                        .or_else(|| self.event_to_series.get(&event_key).cloned())
                        .unwrap_or_else(|| {
                            format!(
                                "{}_fallback:{}:{}",
                                ex,
                                Self::fallback_series_slug(&bo.slug),
                                bo.id
                            )
                        });
                    self.event_to_series.insert(event_key, series_key.clone());

                    // Use slug from instrument for file naming
                    if !bo.slug.is_empty() {
                        self.current_event_slug
                            .insert(series_key.clone(), bo.slug.clone());
                    }

                    // If no EventStart arrived, create an event-scoped fallback
                    // that cannot leak into the next Instrument.
                    if !self.current_event_id.contains_key(&series_key) {
                        self.current_event_id
                            .insert(series_key.clone(), bo.id.clone());
                        if !self.current_series_slug.contains_key(&series_key) {
                            self.current_series_slug
                                .insert(series_key.clone(), Self::fallback_series_slug(&bo.slug));
                        }
                    }

                    // Map all token IDs to this event's file key
                    let eid = self
                        .current_event_id
                        .get(&series_key)
                        .cloned()
                        .unwrap_or_default();
                    let slug = self
                        .current_event_slug
                        .get(&series_key)
                        .cloned()
                        .unwrap_or_default();
                    let series = self
                        .current_series_slug
                        .get(&series_key)
                        .cloned()
                        .unwrap_or_default();
                    let file_key = format!("{}_{}", eid, slug);
                    for token_id in &bo.clob_token_ids {
                        self.token_to_file_key
                            .insert(token_id.clone(), file_key.clone());
                    }
                    let path = self.poly_path(&series, &eid, &slug);
                    let buf = self.get_or_create_buffer(&file_key, path);

                    // Record instrument event
                    let local_ts = crate::types::now_ns();
                    buf.prepare_for_row()?;
                    buf.push_instrument(local_ts, local_ts, &ex, event);
                    buf.flush_if_full()?;
                    self.total_event_count += 1;
                }
            }
            MarketEvent::OrderBook(ob) => {
                let ex = ob.exchange.to_string();
                if let Some((file_key, path)) =
                    self.resolve_file(&ex, &ob.symbol, ob.local_timestamp_ns)
                {
                    self.rotate_buffer(&file_key);
                    let buf = self
                        .buffers
                        .entry(file_key)
                        .or_insert_with(|| ParquetBuffer::new(path));
                    buf.prepare_for_row()?;
                    buf.push_orderbook(
                        ob.exchange_timestamp_ns,
                        ob.local_timestamp_ns,
                        &ex,
                        &ob.symbol,
                        ob,
                    );
                    buf.flush_if_full()?;
                    self.total_event_count += 1;
                }
            }
            MarketEvent::Trade(t) => {
                let ex = t.exchange.to_string();
                if let Some((file_key, path)) =
                    self.resolve_file(&ex, &t.symbol, t.local_timestamp_ns)
                {
                    self.rotate_buffer(&file_key);
                    let buf = self
                        .buffers
                        .entry(file_key)
                        .or_insert_with(|| ParquetBuffer::new(path));
                    buf.prepare_for_row()?;
                    buf.push_trade(t.exchange_timestamp_ns, t.local_timestamp_ns, &ex, t);
                    buf.flush_if_full()?;
                    self.total_event_count += 1;
                }
            }
            MarketEvent::AssetCtx(ac) => {
                let ex = ac.exchange.to_string();
                if let Some((file_key, path)) =
                    self.resolve_file(&ex, &ac.symbol, ac.local_timestamp_ns)
                {
                    self.rotate_buffer(&file_key);
                    let buf = self
                        .buffers
                        .entry(file_key)
                        .or_insert_with(|| ParquetBuffer::new(path));
                    buf.prepare_for_row()?;
                    buf.push_asset_ctx(ac.local_timestamp_ns, ac.local_timestamp_ns, &ex, ac);
                    buf.flush_if_full()?;
                    self.total_event_count += 1;
                }
            }
            MarketEvent::Quote(q) => {
                let ex = q.exchange.to_string();
                if let Some((file_key, path)) =
                    self.resolve_file(&ex, &q.symbol, q.local_timestamp_ns)
                {
                    self.rotate_buffer(&file_key);
                    let buf = self
                        .buffers
                        .entry(file_key)
                        .or_insert_with(|| ParquetBuffer::new(path));
                    buf.prepare_for_row()?;
                    buf.push_quote(q.exchange_timestamp_ns, q.local_timestamp_ns, &ex, q);
                    buf.flush_if_full()?;
                    self.total_event_count += 1;
                }
            }
            MarketEvent::TickSizeChange(tsc) => {
                let ex = tsc.exchange.to_string();
                if let Some((file_key, path)) =
                    self.resolve_file(&ex, &tsc.symbol, tsc.local_timestamp_ns)
                {
                    self.rotate_buffer(&file_key);
                    let buf = self
                        .buffers
                        .entry(file_key)
                        .or_insert_with(|| ParquetBuffer::new(path));
                    buf.prepare_for_row()?;
                    buf.push_tick_size_change(
                        tsc.local_timestamp_ns,
                        tsc.local_timestamp_ns,
                        &ex,
                        tsc,
                    );
                    buf.flush_if_full()?;
                    self.total_event_count += 1;
                }
            }
            MarketEvent::MarketDataHealth(health) => {
                let ex = health.exchange.to_string();
                if let Some((file_key, path)) =
                    self.resolve_file(&ex, &health.symbol, health.local_timestamp_ns)
                {
                    self.rotate_buffer(&file_key);
                    let buf = self
                        .buffers
                        .entry(file_key)
                        .or_insert_with(|| ParquetBuffer::new(path));
                    buf.prepare_for_row()?;
                    buf.push_market_data_health(
                        health.local_timestamp_ns,
                        &ex,
                        &health.symbol,
                        event,
                    );
                    buf.flush_if_full()?;
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
                let key = format!(
                    "{}_{}_{}",
                    source_dir,
                    sym_lower,
                    sp.local_timestamp_ns / 3_600_000_000_000
                );
                let secs = (sp.local_timestamp_ns / 1_000_000_000) as i64;
                let hour_secs = secs - (secs % 3600);
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(hour_secs, 0)
                    .unwrap_or_default();
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
                let buf = self
                    .buffers
                    .entry(key)
                    .or_insert_with(|| ParquetBuffer::new(path));
                buf.prepare_for_row()?;
                buf.push_spot_price(sp.timestamp_ns, sp.local_timestamp_ns, sp);
                buf.flush_if_full()?;
                self.total_event_count += 1;
            }
            MarketEvent::EventEnd {
                exchange,
                event_id,
                retired_symbols,
                ..
            } => {
                self.retire_event_context(&exchange.to_string(), event_id, retired_symbols);
            }
            MarketEvent::Connected { .. }
            | MarketEvent::Disconnected { .. }
            | MarketEvent::Exit => {}
        }

        self.publish_stats();
        Ok(())
    }

    /// Periodic memory bound: close every partial shard. The Arrow batch is
    /// dropped before this method returns; no completed batch remains resident.
    pub fn flush_buffers(&mut self) {
        for buf in self.buffers.values_mut() {
            if !buf.timestamp_ns.is_empty() {
                if let Err(e) = buf.write_pending_shards() {
                    log::error!("[Recorder] Periodic shard write error: {}", e);
                }
            }
        }
        self.publish_stats();
    }

    /// Checkpoint every partial fixed-capacity row group to an immutable shard.
    /// Completed shards are already durable and are never read or rewritten.
    pub fn checkpoint(&mut self) {
        for buf in self.buffers.values_mut() {
            if let Err(e) = buf.write_pending_shards() {
                log::error!(
                    "[Recorder] checkpoint shard write failed for {}: {}",
                    buf.path.display(),
                    e,
                );
            }
        }
        self.publish_stats();
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
        self.publish_stats();
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

        let hist_dir = self
            .output_dir
            .join("histdata")
            .join(exchange_str)
            .join(symbol)
            .join(interval);

        match crate::recorder::hist_reader::save_bars_to_local(&hist_dir, &bars, interval) {
            Ok(()) => {}
            Err(e) => {
                warn!(
                    "[Recorder] Failed to save {} bars for {}: {}",
                    bars.len(),
                    key,
                    e
                );
            }
        }

        Ok(())
    }

    pub fn event_count(&self) -> u64 {
        self.total_event_count
    }
}

impl Drop for MarketRecorder {
    fn drop(&mut self) {
        RECORDER_BUFFERED_ROWS.store(0, Ordering::Release);
        RECORDER_BUFFERED_BYTES.store(0, Ordering::Release);
        RECORDER_OPEN_STREAMS.store(0, Ordering::Release);
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

    fn binary_option(id: &str, slug: &str, token: &str) -> MarketEvent {
        MarketEvent::Instrument(crate::types::Instrument::BinaryOption(
            crate::types::BinaryOption {
                exchange: crate::types::Exchange::Polymarket,
                id: id.to_string(),
                question: String::new(),
                condition_id: id.to_string(),
                series_slug: "btc-updown-5m".to_string(),
                slug: slug.to_string(),
                clob_token_ids: vec![token.to_string()],
                outcomes: vec!["Up".to_string()],
                outcome_prices: vec!["0.5".to_string()],
                active: true,
                closed: false,
                volume: 0.0,
                liquidity: 0.0,
                tick_size: 0.01,
                order_min_size: 1.0,
                group_item_title: String::new(),
                event_start_time: String::new(),
                base_fee: 0,
                fee_exponent: 0.0,
                fee_rate: 0.0,
            },
        ))
    }

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
        assert_eq!(
            comp,
            Compression::SNAPPY,
            "MarketRecorder must default to SNAPPY compression"
        );
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
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::UInt64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![1u64, 2, 3])) as ArrayRef],
        )
        .expect("batch construction");

        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let file = File::create(tmp.path()).expect("create");
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(recorder_writer_properties())).expect("writer");
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
                "column {} ({:?}) must be SNAPPY-compressed",
                ci,
                col.column_path(),
            );
        }
    }

    #[test]
    fn market_data_health_row_preserves_complete_event() {
        let event = MarketEvent::MarketDataHealth(crate::types::MarketDataHealth {
            exchange: crate::types::Exchange::Polymarket,
            market_id: "condition".to_string(),
            symbol: "up-token".to_string(),
            state: crate::types::MarketDataHealthState::Settling,
            passive_ready: true,
            taker_ready: false,
            reason: "BBO checkpoint mismatch".to_string(),
            local_timestamp_ns: 123,
        });
        let tempdir = tempfile::tempdir().expect("tempdir");
        let mut buffer = ParquetBuffer::new(tempdir.path().join("health.parquet"));
        buffer.push_market_data_health(123, "polymarket", "up-token", &event);

        assert_eq!(buffer.event_type, vec!["market_data_health"]);
        assert_eq!(buffer.symbol, vec!["up-token"]);
        let decoded: MarketEvent =
            serde_json::from_str(buffer.data_json[0].as_deref().expect("health JSON"))
                .expect("decode health event");
        let MarketEvent::MarketDataHealth(health) = decoded else {
            panic!("decoded wrong event variant")
        };
        assert_eq!(health.market_id, "condition");
        assert_eq!(health.state, crate::types::MarketDataHealthState::Settling);
        assert!(!health.taker_ready);
    }

    #[test]
    fn full_row_group_is_sharded_and_released_before_more_rows_arrive() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let base = tempdir.path().join("quotes.parquet");
        let mut buffer = ParquetBuffer::new(base);
        let quote = crate::types::QuoteTick {
            exchange: crate::types::Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            bid_price: 100.0,
            bid_qty: 1.0,
            ask_price: 101.0,
            ask_qty: 1.0,
            exchange_timestamp_ns: 1,
            local_timestamp_ns: 1,
        };

        for index in 0..=RECORDER_ROW_GROUP_ROWS {
            buffer.prepare_for_row().expect("prepare row");
            buffer.push_quote(index as u64, index as u64, "binance", &quote);
            buffer.flush_if_full().expect("flush full group");
        }

        assert_eq!(buffer.timestamp_ns.len(), 1);
        assert!(buffer.pending_batch.is_none());
        assert!(tempdir.path().join("quotes.part-000000.parquet").is_file());
        buffer.write_pending_shards().expect("checkpoint tail");
        assert!(buffer.timestamp_ns.is_empty());
        assert!(buffer.pending_batch.is_none());
        assert!(tempdir.path().join("quotes.part-000001.parquet").is_file());
    }

    #[test]
    fn stale_instrument_cannot_claim_the_next_series_or_shared_fallback() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let mut recorder = MarketRecorder::new(tempdir.path().to_path_buf()).expect("recorder");
        let exchange = crate::types::Exchange::Polymarket;

        recorder
            .write_event(&MarketEvent::EventStart {
                exchange,
                symbol: "series:btc-updown-5m".to_string(),
                event_id: "event-a".to_string(),
                event_start_ns: 1,
            })
            .unwrap();
        recorder
            .write_event(&binary_option(
                "event-a",
                "btc-updown-5m-1774807800",
                "token-a",
            ))
            .unwrap();
        assert!(recorder.token_to_file_key.contains_key("token-a"));

        recorder
            .write_event(&MarketEvent::EventEnd {
                exchange,
                symbol: "series:btc-updown-5m".to_string(),
                event_id: "event-a".to_string(),
                retired_symbols: vec!["token-a".to_string()],
                event_end_ns: 2,
            })
            .unwrap();
        recorder
            .write_event(&MarketEvent::EventStart {
                exchange,
                symbol: "series:btc-updown-5m".to_string(),
                event_id: "event-b".to_string(),
                event_start_ns: 3,
            })
            .unwrap();

        recorder
            .write_event(&binary_option(
                "event-a",
                "btc-updown-5m-1774807800",
                "stale-token-a",
            ))
            .unwrap();
        assert!(!recorder.token_to_file_key.contains_key("stale-token-a"));
        assert_eq!(
            recorder
                .current_event_id
                .get("polymarket_btc-updown-5m")
                .map(String::as_str),
            Some("event-b")
        );

        recorder
            .write_event(&binary_option(
                "event-b",
                "btc-updown-5m-1774808100",
                "token-b",
            ))
            .unwrap();
        recorder
            .write_event(&binary_option(
                "event-c",
                "eth-updown-5m-1774808400",
                "token-c",
            ))
            .unwrap();
        recorder
            .write_event(&binary_option(
                "event-d",
                "sol-updown-5m-1774808700",
                "token-d",
            ))
            .unwrap();

        assert_ne!(
            recorder.token_to_file_key.get("token-c"),
            recorder.token_to_file_key.get("token-d")
        );
        assert_ne!(
            recorder
                .event_to_series
                .get(&MarketRecorder::event_key("polymarket", "event-c")),
            recorder
                .event_to_series
                .get(&MarketRecorder::event_key("polymarket", "event-d"))
        );
    }

    #[test]
    fn instrument_before_start_is_closed_by_matching_event_end() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let mut recorder = MarketRecorder::new(tempdir.path().to_path_buf()).expect("recorder");
        recorder
            .write_event(&binary_option(
                "event-early",
                "btc-updown-5m-1774809000",
                "token-early",
            ))
            .unwrap();
        let file_key = recorder
            .token_to_file_key
            .get("token-early")
            .cloned()
            .expect("early token mapping");
        assert!(recorder.buffers.contains_key(&file_key));

        recorder
            .write_event(&MarketEvent::EventStart {
                exchange: crate::types::Exchange::Polymarket,
                symbol: "series:btc-updown-5m".to_string(),
                event_id: "event-early".to_string(),
                event_start_ns: 1,
            })
            .unwrap();
        recorder
            .write_event(&MarketEvent::EventEnd {
                exchange: crate::types::Exchange::Polymarket,
                symbol: "series:btc-updown-5m".to_string(),
                event_id: "event-early".to_string(),
                retired_symbols: vec!["token-early".to_string()],
                event_end_ns: 2,
            })
            .unwrap();

        assert!(!recorder.buffers.contains_key(&file_key));
        assert!(!recorder.token_to_file_key.contains_key("token-early"));
        assert!(!recorder
            .event_to_series
            .contains_key(&MarketRecorder::event_key("polymarket", "event-early")));
    }
}
