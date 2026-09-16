//! Parquet-based market data replay reader.
//!
//! Reads Parquet files recorded by the writer, reconstructs MarketEvent objects,
//! and replays them ordered by local_timestamp_ns.

use anyhow::{anyhow, Result};
use arrow::array::*;
use chrono::{DateTime, Utc};
use log::{info, warn};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use crate::types::*;

/// A single row from a Parquet market data file, ready for replay.
#[derive(Clone)]
struct ReplayRow {
    local_timestamp_ns: u64,
    event: MarketEvent,
}

/// Optional, explicitly bounded repairs for legacy rotating-event tapes.
///
/// Older Polymarket recordings sometimes registered an event only after the
/// event had opened, so the first instrument/book rows are 1--4 seconds late.
/// For those tapes we can move the instrument to the scheduled open and seed a
/// synthetic opening snapshot from the first recorded full book.  This is
/// deliberately opt-in: complete tapes replay byte-for-byte as before.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayOptions {
    pub time_policy: ReplayTimePolicy,
    pub bootstrap_binary_open: bool,
    pub binary_open_delay_ns: u64,
    pub binary_open_max_backfill_ns: u64,
}

/// Which clock partitions warmup and strategy replay. The strict policy uses
/// recorded receive visibility for *all* rows, including instruments. Callers
/// must use the same policy for adjacent warmup/replay windows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReplayTimePolicy {
    #[default]
    LegacySourceTime,
    ArrivalTimeStrict,
}

impl ReplayOptions {
    fn validate(self) -> Result<()> {
        if self.time_policy == ReplayTimePolicy::ArrivalTimeStrict && self.bootstrap_binary_open {
            return Err(anyhow!(
                "ArrivalTimeStrict is incompatible with future binary-open bootstrap"
            ));
        }
        Ok(())
    }
}

const REPLAYER_BATCH_ROWS: usize = 8_192;
const REPLAYER_BOOTSTRAP_MAX_ROWS: usize = REPLAYER_BATCH_ROWS * 2;
// The bootstrap accumulator checks its bound after appending one decoded Arrow
// batch, so the emitted repaired batch can contain the prior 2-batch window
// plus one final batch (observed 22,792 rows on legacy Polymarket tapes).
const REPLAY_CACHE_MAX_BATCH_ROWS: usize = REPLAYER_BOOTSTRAP_MAX_ROWS + REPLAYER_BATCH_ROWS;
const REPLAY_CACHE_MAGIC: [u8; 8] = *b"HXRPLY01";
const REPLAY_CACHE_VERSION: u32 = 2;
const REPLAY_CACHE_HEADER_BYTES: usize = 48;
const REPLAY_CACHE_MAX_EVENT_BYTES: usize = 16 * 1024 * 1024;

static REPLAYER_BUFFERED_ROWS: AtomicU64 = AtomicU64::new(0);
static REPLAYER_BUFFER_CAPACITY: AtomicU64 = AtomicU64::new(0);
static REPLAYER_LOADED_ROWS: AtomicU64 = AtomicU64::new(0);
static REPLAYER_ACTIVE_STREAMS: AtomicU64 = AtomicU64::new(0);
static REPLAY_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static REPLAY_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static REPLAY_CACHE_WRITES: AtomicU64 = AtomicU64::new(0);
static REPLAY_CACHE_ROWS: AtomicU64 = AtomicU64::new(0);
static REPLAY_CACHE_READ_NS: AtomicU64 = AtomicU64::new(0);
static REPLAY_PARQUET_DECODE_NS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReplayCacheMode {
    #[default]
    Off,
    ReadWrite,
    Refresh,
}

impl ReplayCacheMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "off" => Ok(Self::Off),
            "read_write" | "read-write" | "rw" => Ok(Self::ReadWrite),
            "refresh" => Ok(Self::Refresh),
            other => Err(anyhow!(
                "invalid backtest replay_cache_mode={other}; expected off, read_write, or refresh"
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayCacheConfig {
    mode: ReplayCacheMode,
    directory: PathBuf,
}

static REPLAY_CACHE_CONFIG: OnceLock<ReplayCacheConfig> = OnceLock::new();

/// Install immutable process-wide replay-cache settings before any replayer is
/// constructed. The cache is consumed exclusively by background loader
/// threads; strategies and execution lanes never access it.
pub fn configure_replay_cache(mode: ReplayCacheMode, directory: PathBuf) -> Result<()> {
    let requested = ReplayCacheConfig { mode, directory };
    if let Some(existing) = REPLAY_CACHE_CONFIG.get() {
        return if existing == &requested {
            Ok(())
        } else {
            Err(anyhow!("replay cache already configured as {:?}", existing))
        };
    }
    if mode != ReplayCacheMode::Off {
        std::fs::create_dir_all(&requested.directory).map_err(|error| {
            anyhow!(
                "create replay cache directory {}: {error}",
                requested.directory.display()
            )
        })?;
    }
    REPLAY_CACHE_CONFIG
        .set(requested)
        .map_err(|_| anyhow!("replay cache was configured more than once"))
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayerStats {
    pub buffered_rows: u64,
    pub buffer_capacity: u64,
    pub loaded_rows: u64,
    pub active_streams: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_writes: u64,
    pub cache_rows: u64,
    pub cache_read_ns: u64,
    pub parquet_decode_ns: u64,
}

pub fn replayer_stats() -> ReplayerStats {
    ReplayerStats {
        buffered_rows: REPLAYER_BUFFERED_ROWS.load(Ordering::Acquire),
        buffer_capacity: REPLAYER_BUFFER_CAPACITY.load(Ordering::Acquire),
        loaded_rows: REPLAYER_LOADED_ROWS.load(Ordering::Acquire),
        active_streams: REPLAYER_ACTIVE_STREAMS.load(Ordering::Acquire),
        cache_hits: REPLAY_CACHE_HITS.load(Ordering::Acquire),
        cache_misses: REPLAY_CACHE_MISSES.load(Ordering::Acquire),
        cache_writes: REPLAY_CACHE_WRITES.load(Ordering::Acquire),
        cache_rows: REPLAY_CACHE_ROWS.load(Ordering::Acquire),
        cache_read_ns: REPLAY_CACHE_READ_NS.load(Ordering::Acquire),
        parquet_decode_ns: REPLAY_PARQUET_DECODE_NS.load(Ordering::Acquire),
    }
}

struct ReplayBatch {
    rows: Vec<ReplayRow>,
}

impl ReplayBatch {
    fn new(rows: Vec<ReplayRow>) -> Self {
        REPLAYER_BUFFERED_ROWS.fetch_add(rows.len() as u64, Ordering::AcqRel);
        REPLAYER_BUFFER_CAPACITY.fetch_add(rows.capacity() as u64, Ordering::AcqRel);
        Self { rows }
    }
}

impl Drop for ReplayBatch {
    fn drop(&mut self) {
        REPLAYER_BUFFERED_ROWS.fetch_sub(self.rows.len() as u64, Ordering::AcqRel);
        REPLAYER_BUFFER_CAPACITY.fetch_sub(self.rows.capacity() as u64, Ordering::AcqRel);
    }
}

enum ReplayCacheSource {
    Hit {
        path: PathBuf,
        fingerprint: [u8; 32],
    },
    Parquet {
        writer: Option<ReplayCacheWriter>,
    },
}

struct ReplayCacheWriter {
    final_path: PathBuf,
    temp_path: PathBuf,
    writer: Option<BufWriter<File>>,
    records: u64,
}

impl ReplayCacheWriter {
    fn create(final_path: PathBuf, fingerprint: [u8; 32]) -> Result<Self> {
        static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp_path =
            final_path.with_extension(format!("bin.{}.{}.tmp", std::process::id(), sequence,));
        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        writer.write_all(&REPLAY_CACHE_MAGIC)?;
        writer.write_all(&REPLAY_CACHE_VERSION.to_le_bytes())?;
        writer.write_all(&0_u32.to_le_bytes())?;
        writer.write_all(&fingerprint)?;
        Ok(Self {
            final_path,
            temp_path,
            writer: Some(writer),
            records: 0,
        })
    }

    fn write_rows(&mut self, rows: &[ReplayRow]) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("replay cache writer is closed"))?;
        if rows.is_empty() || rows.len() > REPLAY_CACHE_MAX_BATCH_ROWS {
            return Err(anyhow!("invalid replay cache batch length {}", rows.len()));
        }
        writer.write_all(&(rows.len() as u32).to_le_bytes())?;
        for row in rows {
            let payload = rmp_serde::to_vec(&row.event)?;
            if payload.len() > REPLAY_CACHE_MAX_EVENT_BYTES {
                return Err(anyhow!(
                    "replay event payload too large: {} bytes",
                    payload.len()
                ));
            }
            writer.write_all(&row.local_timestamp_ns.to_le_bytes())?;
            writer.write_all(&(payload.len() as u32).to_le_bytes())?;
            writer.write_all(&payload)?;
            self.records = self.records.saturating_add(1);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<u64> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| anyhow!("replay cache writer is closed"))?;
        writer.flush()?;
        drop(writer);
        std::fs::rename(&self.temp_path, &self.final_path)?;
        REPLAY_CACHE_WRITES.fetch_add(1, Ordering::AcqRel);
        Ok(self.records)
    }
}

impl Drop for ReplayCacheWriter {
    fn drop(&mut self) {
        if self.writer.is_some() {
            self.writer = None;
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

fn replay_cache_source(
    files: &[PathBuf],
    source: &str,
    start_ns: u64,
    end_ns: u64,
    options: ReplayOptions,
) -> ReplayCacheSource {
    let Some(config) = REPLAY_CACHE_CONFIG.get() else {
        return ReplayCacheSource::Parquet { writer: None };
    };
    if config.mode == ReplayCacheMode::Off {
        return ReplayCacheSource::Parquet { writer: None };
    }
    let fingerprint = replay_cache_fingerprint(files, source, start_ns, end_ns, options);
    let final_path = config
        .directory
        .join(format!("{}.replay.bin", hex::encode(fingerprint)));
    if config.mode == ReplayCacheMode::ReadWrite
        && replay_cache_header_matches(&final_path, fingerprint)
    {
        return ReplayCacheSource::Hit {
            path: final_path,
            fingerprint,
        };
    }
    REPLAY_CACHE_MISSES.fetch_add(1, Ordering::AcqRel);
    let writer = match ReplayCacheWriter::create(final_path.clone(), fingerprint) {
        Ok(writer) => Some(writer),
        Err(error) => {
            warn!(
                "[ReplayerCache] cannot create {}: {}; replaying parquet without cache",
                final_path.display(),
                error,
            );
            None
        }
    };
    ReplayCacheSource::Parquet { writer }
}

fn replay_cache_fingerprint(
    files: &[PathBuf],
    source: &str,
    start_ns: u64,
    end_ns: u64,
    options: ReplayOptions,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"hexagent-replay-cache-v2-batched-market-event-rmp");
    hash.update(source.as_bytes());
    hash.update(start_ns.to_le_bytes());
    hash.update(end_ns.to_le_bytes());
    hash.update([
        options.bootstrap_binary_open as u8,
        options.time_policy as u8,
        0,
        0,
        0,
        0,
        0,
        0,
    ]);
    hash.update(options.binary_open_delay_ns.to_le_bytes());
    hash.update(options.binary_open_max_backfill_ns.to_le_bytes());
    for path in files {
        hash.update(path.as_os_str().as_encoded_bytes());
        match std::fs::metadata(path) {
            Ok(metadata) => {
                hash.update(metadata.len().to_le_bytes());
                if let Ok(modified) = metadata.modified() {
                    if let Ok(duration) = modified.duration_since(std::time::UNIX_EPOCH) {
                        hash.update(duration.as_secs().to_le_bytes());
                        hash.update(duration.subsec_nanos().to_le_bytes());
                    }
                }
            }
            Err(error) => hash.update(error.to_string().as_bytes()),
        }
    }
    hash.finalize().into()
}

fn replay_cache_header_matches(path: &Path, fingerprint: [u8; 32]) -> bool {
    let Ok(file) = File::open(path) else {
        return false;
    };
    if file.metadata().map_or(true, |metadata| {
        metadata.len() < REPLAY_CACHE_HEADER_BYTES as u64
    }) {
        return false;
    }
    let mut reader = BufReader::new(file);
    let mut header = [0_u8; REPLAY_CACHE_HEADER_BYTES];
    reader.read_exact(&mut header).is_ok()
        && header[0..8] == REPLAY_CACHE_MAGIC
        && u32::from_le_bytes(header[8..12].try_into().expect("fixed cache header"))
            == REPLAY_CACHE_VERSION
        && header[12..16] == [0; 4]
        && header[16..48] == fingerprint
}

fn stream_replay_cache(
    path: &Path,
    fingerprint: [u8; 32],
    batch_tx: &crossbeam_channel::Sender<std::result::Result<ReplayBatch, String>>,
) -> Result<u64> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut header = [0_u8; REPLAY_CACHE_HEADER_BYTES];
    reader.read_exact(&mut header)?;
    if header[0..8] != REPLAY_CACHE_MAGIC
        || u32::from_le_bytes(header[8..12].try_into().expect("fixed cache header"))
            != REPLAY_CACHE_VERSION
        || header[16..48] != fingerprint
    {
        return Err(anyhow!("replay cache header mismatch"));
    }
    let mut payload = Vec::with_capacity(1024);
    let mut total = 0_u64;
    loop {
        let mut batch_bytes = [0_u8; 4];
        match reader.read(&mut batch_bytes[..1]) {
            Ok(0) => break,
            Ok(1) => reader.read_exact(&mut batch_bytes[1..])?,
            Ok(_) => unreachable!("single-byte read returned more than one byte"),
            Err(error) => return Err(error.into()),
        }
        let batch_len = u32::from_le_bytes(batch_bytes) as usize;
        if batch_len == 0 || batch_len > REPLAY_CACHE_MAX_BATCH_ROWS {
            return Err(anyhow!("invalid replay cache batch length {batch_len}"));
        }
        let mut rows = Vec::with_capacity(batch_len);
        for _ in 0..batch_len {
            let mut prefix = [0_u8; 12];
            reader.read_exact(&mut prefix)?;
            let local_timestamp_ns = u64::from_le_bytes(prefix[0..8].try_into().unwrap());
            let payload_len = u32::from_le_bytes(prefix[8..12].try_into().unwrap()) as usize;
            if payload_len == 0 || payload_len > REPLAY_CACHE_MAX_EVENT_BYTES {
                return Err(anyhow!("invalid replay cache event length {payload_len}"));
            }
            payload.clear();
            payload.resize(payload_len, 0);
            reader.read_exact(&mut payload)?;
            let event = rmp_serde::from_slice::<MarketEvent>(&payload)?;
            rows.push(ReplayRow {
                local_timestamp_ns,
                event,
            });
        }
        let count = rows.len() as u64;
        REPLAYER_LOADED_ROWS.fetch_add(count, Ordering::AcqRel);
        total = total.saturating_add(count);
        if batch_tx.send(Ok(ReplayBatch::new(rows))).is_err() {
            return Ok(total);
        }
    }
    Ok(total)
}

#[inline]
fn elapsed_nanos(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Reads Parquet market data files and replays events in order. The producer
/// normally hands Arrow-sized batches directly to a two-batch consumer window;
/// the optional opening repair may combine at most two row groups, but an hour
/// is never materialised as one `Vec<ReplayRow>`.
pub struct MarketReplayer {
    batch_rx: Option<crossbeam_channel::Receiver<std::result::Result<ReplayBatch, String>>>,
    loader_handle: Option<std::thread::JoinHandle<()>>,
    current_batch: Option<ReplayBatch>,
    lookahead_batch: Option<ReplayBatch>,
    row_cursor: usize,
    event_count: u64,
    source: String,
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
        Self::new_with_options(
            data_dir,
            exchange,
            symbol,
            start,
            end,
            ReplayOptions::default(),
        )
    }

    /// Create a replayer with bounded legacy-tape repairs.
    pub fn new_with_options(
        data_dir: &Path,
        exchange: &str,
        symbol: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        options: ReplayOptions,
    ) -> Result<Self> {
        let start_ns = start.timestamp_nanos_opt().unwrap_or(0) as u64;
        let end_ns = end.timestamp_nanos_opt().unwrap_or(0) as u64;

        options.validate()?;
        // A source-time filename cannot exclude late arrivals in strict mode.
        // Inspect receive-time footer bounds across every discovered source file;
        // unknown bounds are decoded and validated by the loader, never guessed.
        let discovery_started = Instant::now();
        let all_files = discover_parquet_files(data_dir, exchange, symbol)?;
        let discovered_files = all_files.len();
        let files = select_replay_files(all_files, start_ns, end_ns, options.time_policy)?;
        if options.time_policy == ReplayTimePolicy::ArrivalTimeStrict {
            info!("[Replayer] ArrivalTimeStrict discovery source={exchange}/{symbol} discovered_files={} selected_files={} footer_scan_ms={}",
                discovered_files, files.len(), discovery_started.elapsed().as_millis());
        }

        if files.is_empty() {
            return Err(anyhow!(
                "No Parquet files found for {}/{} in time range",
                exchange,
                symbol
            ));
        }

        info!(
            "[Replayer] Found {} Parquet files for {}/{}",
            files.len(),
            exchange,
            symbol
        );

        let source = format!("{exchange}/{symbol}");
        let cache_source = replay_cache_source(&files, &source, start_ns, end_ns, options);
        let worker_source = source.clone();
        // Rendezvous handoff: at steady state the replayer owns current +
        // lookahead and the loader can hold at most one decoded batch while
        // blocked on send. Opening repair may temporarily merge two row groups;
        // that separate startup bound is REPLAYER_BOOTSTRAP_MAX_ROWS.
        let (batch_tx, batch_rx) = crossbeam_channel::bounded(0);
        let loader_handle = std::thread::Builder::new()
            .name("market-replay-loader".to_string())
            .spawn(move || {
                hexagent_runtime::os_tune::pin_background("market-replay-loader");
                match cache_source {
                    ReplayCacheSource::Hit { path, fingerprint } => {
                        let started = Instant::now();
                        match stream_replay_cache(&path, fingerprint, &batch_tx) {
                            Ok(rows) => {
                                REPLAY_CACHE_HITS.fetch_add(1, Ordering::AcqRel);
                                REPLAY_CACHE_ROWS.fetch_add(rows, Ordering::AcqRel);
                                REPLAY_CACHE_READ_NS
                                    .fetch_add(elapsed_nanos(started), Ordering::AcqRel);
                                info!(
                                    "[ReplayerCache] hit source={} rows={} path={}",
                                    worker_source,
                                    rows,
                                    path.display(),
                                );
                            }
                            Err(error) => {
                                let message = format!(
                                    "replay cache read failed source={} path={}: {}",
                                    worker_source,
                                    path.display(),
                                    error,
                                );
                                warn!("[ReplayerCache] {message}");
                                // Cache corruption must not masquerade as a
                                // clean end-of-stream after a partial replay.
                                let _ = batch_tx.send(Err(message));
                            }
                        }
                    }
                    ReplayCacheSource::Parquet { writer } => {
                        let started = Instant::now();
                        stream_replay_files(
                            files,
                            start_ns,
                            end_ns,
                            &worker_source,
                            options,
                            batch_tx,
                            writer,
                        );
                        REPLAY_PARQUET_DECODE_NS
                            .fetch_add(elapsed_nanos(started), Ordering::AcqRel);
                    }
                }
            })?;
        REPLAYER_ACTIVE_STREAMS.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            batch_rx: Some(batch_rx),
            loader_handle: Some(loader_handle),
            current_batch: None,
            lookahead_batch: None,
            row_cursor: 0,
            event_count: 0,
            source,
        })
    }

    /// Create a timestamp-ordered replayer from a CSV health sidecar.
    ///
    /// Schema:
    /// `timestamp_ns,market_id,state,passive_ready,taker_ready,reason`
    ///
    /// This is intentionally a startup-only loader.  It is used to replay
    /// legacy operational evidence that was logged separately from the market
    /// parquet, while newly recorded tapes carry the same event natively.
    pub fn from_market_data_health_csv(
        path: &Path,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Self> {
        let start_ns = start.timestamp_nanos_opt().unwrap_or(0) as u64;
        let end_ns = end.timestamp_nanos_opt().unwrap_or(0) as u64;
        let text = std::fs::read_to_string(path)
            .map_err(|error| anyhow!("cannot read health replay {}: {}", path.display(), error))?;
        let mut rows = Vec::new();
        for (line_index, raw_line) in text.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with("timestamp_ns,") {
                continue;
            }
            let fields: Vec<&str> = line.splitn(6, ',').map(str::trim).collect();
            if fields.len() != 6 {
                return Err(anyhow!(
                    "{}:{} expected 6 health replay columns, found {}",
                    path.display(),
                    line_index + 1,
                    fields.len(),
                ));
            }
            let local_timestamp_ns = fields[0].parse::<u64>().map_err(|error| {
                anyhow!(
                    "{}:{} invalid timestamp_ns: {}",
                    path.display(),
                    line_index + 1,
                    error,
                )
            })?;
            if local_timestamp_ns < start_ns || local_timestamp_ns >= end_ns {
                continue;
            }
            let state = match fields[2].to_ascii_lowercase().as_str() {
                "healthy" => MarketDataHealthState::Healthy,
                "settling" => MarketDataHealthState::Settling,
                "repairing" => MarketDataHealthState::Repairing,
                "degraded" => MarketDataHealthState::Degraded,
                other => {
                    return Err(anyhow!(
                        "{}:{} invalid health state {}",
                        path.display(),
                        line_index + 1,
                        other,
                    ))
                }
            };
            let parse_bool = |value: &str, name: &str| -> Result<bool> {
                match value.to_ascii_lowercase().as_str() {
                    "true" | "1" => Ok(true),
                    "false" | "0" => Ok(false),
                    other => Err(anyhow!(
                        "{}:{} invalid {} {}",
                        path.display(),
                        line_index + 1,
                        name,
                        other,
                    )),
                }
            };
            rows.push(ReplayRow {
                local_timestamp_ns,
                event: MarketEvent::MarketDataHealth(MarketDataHealth {
                    exchange: Exchange::Polymarket,
                    market_id: fields[1].to_string(),
                    // Condition id is sufficient for strategy-local routing in
                    // the single-thread backtest driver. Live/worker routing
                    // continues to use the canonical token symbol from native
                    // health events.
                    symbol: String::new(),
                    state,
                    passive_ready: parse_bool(fields[3], "passive_ready")?,
                    taker_ready: parse_bool(fields[4], "taker_ready")?,
                    reason: fields[5].to_string(),
                    local_timestamp_ns,
                }),
            });
        }
        rows.sort_by_key(|row| row.local_timestamp_ns);
        if rows.is_empty() {
            return Err(anyhow!(
                "No MarketDataHealth rows found in {} for time range",
                path.display(),
            ));
        }
        let loaded_rows = rows.len() as u64;
        info!(
            "[Replayer] Loaded {} MarketDataHealth sidecar rows from {}",
            loaded_rows,
            path.display(),
        );
        REPLAYER_LOADED_ROWS.fetch_add(loaded_rows, Ordering::AcqRel);
        REPLAYER_ACTIVE_STREAMS.fetch_add(1, Ordering::AcqRel);
        Ok(Self {
            batch_rx: None,
            loader_handle: None,
            current_batch: Some(ReplayBatch::new(rows)),
            lookahead_batch: None,
            row_cursor: 0,
            event_count: 0,
            source: format!("health-sidecar/{}", path.display()),
        })
    }

    fn load_next_batch(&mut self) -> Result<bool> {
        self.current_batch = None;
        self.row_cursor = 0;
        if let Some(batch) = self.lookahead_batch.take() {
            self.current_batch = Some(batch);
        }
        let Some(rx) = self.batch_rx.as_ref() else {
            return Ok(self.current_batch.is_some());
        };
        if self.current_batch.is_none() {
            self.current_batch = match rx.recv() {
                Ok(Ok(batch)) => Some(batch),
                Ok(Err(message)) => return Err(anyhow!(message)),
                Err(_) => None,
            };
        }
        if self.current_batch.is_some() && self.lookahead_batch.is_none() {
            self.lookahead_batch = match rx.recv() {
                Ok(Ok(batch)) => Some(batch),
                Ok(Err(message)) => return Err(anyhow!(message)),
                Err(_) => None,
            };
        }
        Ok(self.current_batch.is_some())
    }

    fn unconsumed_rows(&self) -> impl Iterator<Item = &ReplayRow> {
        self.current_batch
            .iter()
            .flat_map(|batch| batch.rows[self.row_cursor..].iter())
            .chain(
                self.lookahead_batch
                    .iter()
                    .flat_map(|batch| batch.rows.iter()),
            )
    }

    /// Get next event with its recorded local timestamp, optionally simulating inter-event timing.
    pub fn next_event(&mut self) -> Result<Option<(u64, MarketEvent)>> {
        let exhausted = self
            .current_batch
            .as_ref()
            .is_none_or(|batch| self.row_cursor >= batch.rows.len());
        if exhausted {
            if !self.load_next_batch()? {
                return Ok(None);
            }
        }

        // Take ownership of event instead of cloning (avoids heap allocation per event)
        let row = &mut self.current_batch.as_mut().unwrap().rows[self.row_cursor];
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
        for row in self.unconsumed_rows().take(MAX_SCAN) {
            if row.local_timestamp_ns < target_ns {
                continue;
            }
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
    ) -> Option<(
        u64,
        Vec<crate::types::PriceLevel>,
        Vec<crate::types::PriceLevel>,
    )> {
        const MAX_SCAN: usize = 16384;
        for row in self.unconsumed_rows().take(MAX_SCAN) {
            if let crate::types::MarketEvent::OrderBook(ob) = &row.event {
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
    ) -> Option<(
        u64,
        &[crate::types::PriceLevel],
        &[crate::types::PriceLevel],
    )> {
        const MAX_SCAN: usize = 16384;
        for row in self.unconsumed_rows().take(MAX_SCAN) {
            if let crate::types::MarketEvent::OrderBook(ob) = &row.event {
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
    ) -> Vec<(
        u64,
        Vec<crate::types::PriceLevel>,
        Vec<crate::types::PriceLevel>,
    )> {
        const MAX_SCAN: usize = 16384;
        let mut out = Vec::new();
        for row in self.unconsumed_rows().take(MAX_SCAN) {
            if let crate::types::MarketEvent::OrderBook(ob) = &row.event {
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

impl Drop for MarketReplayer {
    fn drop(&mut self) {
        // Drop the receiver first so a producer blocked on the two-batch lane
        // wakes immediately, then release the current decoded batch and join.
        self.batch_rx = None;
        self.current_batch = None;
        self.lookahead_batch = None;
        if let Some(handle) = self.loader_handle.take() {
            let _ = handle.join();
        }
        REPLAYER_ACTIVE_STREAMS.fetch_sub(1, Ordering::AcqRel);
        log::debug!(
            "[Replayer] closed source={} emitted_rows={}",
            self.source,
            self.event_count
        );
    }
}

/// Only use trustworthy receive bounds to exclude a file. Unknown/null/zero
/// provenance forces decoding (and a clear strict-mode error if invalid).
fn row_group_receive_bounds(
    group: &parquet::file::metadata::RowGroupMetaData,
) -> Option<(u64, u64)> {
    use parquet::file::statistics::Statistics;
    let column = group
        .columns()
        .iter()
        .find(|column| column.column_descr().path().string() == "local_timestamp_ns")?;
    let Statistics::Int64(stats) = column.statistics()? else {
        return None;
    };
    if stats.null_count_opt().unwrap_or(1) != 0 {
        return None;
    }
    let (&min, &max) = (stats.min_opt()?, stats.max_opt()?);
    if min <= 0 || max < min {
        return None;
    }
    Some((min as u64, max as u64))
}

fn recorded_receive_bounds(path: &Path) -> Result<Option<(u64, u64)>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let mut bounds: Option<(u64, u64)> = None;
    for group in builder.metadata().row_groups() {
        if group.num_rows() == 0 {
            continue;
        }
        let Some((min, max)) = row_group_receive_bounds(group) else {
            return Ok(None);
        };
        bounds = Some(match bounds {
            Some((previous_min, previous_max)) => (previous_min.min(min), previous_max.max(max)),
            None => (min, max),
        });
    }
    Ok(bounds)
}

fn select_replay_files(
    all_files: Vec<PathBuf>,
    start_ns: u64,
    end_ns: u64,
    policy: ReplayTimePolicy,
) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for path in all_files {
        let include = match policy {
            ReplayTimePolicy::LegacySourceTime => match extract_file_timestamp(&path) {
                Some((ts, duration)) => {
                    ts + duration > start_ns / 1_000_000_000 && ts < end_ns / 1_000_000_000
                }
                None => true,
            },
            ReplayTimePolicy::ArrivalTimeStrict => match recorded_receive_bounds(&path) {
                Ok(Some((min, max))) => max >= start_ns && min < end_ns,
                Ok(None) => true,
                Err(error) => {
                    return Err(anyhow!(
                        "cannot establish receive coverage for {}: {error}",
                        path.display()
                    ))
                }
            },
        };
        if include {
            files.push(path);
        }
    }
    Ok(files)
}

// Strict replay uses bounded offline merge runs. It processes disjoint receive
// intervals one at a time, so ordinary hourly/sharded tapes start after the first
// interval, not after decoding the whole replay window. Overlapping filenames or
// unknown footer bounds are merged before delivery; filenames are never clocks.
const STRICT_MERGE_FAN_IN: usize = 32;
// Merge output may temporarily double these bytes, plus one bounded event.
// Ordinary non-overlapping row groups do not create any scratch runs.
const STRICT_MAX_GROUP_RUN_BYTES: u64 = 512 * 1024 * 1024;
struct StrictScratch {
    directory: PathBuf,
    next_run: usize,
}
impl StrictScratch {
    fn new() -> Result<Self> {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "hexagent-arrival-replay-{}-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
            uuid::Uuid::new_v4(),
        ));
        std::fs::create_dir(&directory)?;
        Ok(Self {
            directory,
            next_run: 0,
        })
    }
    fn next_path(&mut self) -> PathBuf {
        let path = self.directory.join(format!("{:010}.run", self.next_run));
        self.next_run += 1;
        path
    }
}
impl Drop for StrictScratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn write_strict_row(writer: &mut impl Write, row: &ReplayRow) -> Result<u64> {
    let payload = rmp_serde::to_vec(&row.event)?;
    if payload.is_empty() || payload.len() > REPLAY_CACHE_MAX_EVENT_BYTES {
        return Err(anyhow!(
            "invalid strict replay event size {}",
            payload.len()
        ));
    }
    writer.write_all(&row.local_timestamp_ns.to_le_bytes())?;
    writer.write_all(&(payload.len() as u32).to_le_bytes())?;
    writer.write_all(&payload)?;
    Ok(12 + payload.len() as u64)
}

struct StrictRunReader {
    reader: BufReader<File>,
    payload: Vec<u8>,
}
impl StrictRunReader {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, File::open(path)?),
            payload: Vec::new(),
        })
    }
    fn next(&mut self) -> Result<Option<ReplayRow>> {
        let mut prefix = [0_u8; 12];
        if self.reader.read(&mut prefix[..1])? == 0 {
            return Ok(None);
        }
        self.reader.read_exact(&mut prefix[1..])?;
        let local_timestamp_ns = u64::from_le_bytes(prefix[..8].try_into().unwrap());
        let length = u32::from_le_bytes(prefix[8..].try_into().unwrap()) as usize;
        if length == 0 || length > REPLAY_CACHE_MAX_EVENT_BYTES {
            return Err(anyhow!("invalid strict replay run event length {length}"));
        }
        self.payload.resize(length, 0);
        self.reader.read_exact(&mut self.payload)?;
        let event = rmp_serde::from_slice(&self.payload)?;
        Ok(Some(ReplayRow {
            local_timestamp_ns,
            event,
        }))
    }
}

/// Contiguous input run order is the stable tie-breaker. Multi-pass merging
/// therefore retains original discovered-file order and row order at equal ns.
fn merge_strict_runs(
    paths: &[PathBuf],
    mut emit: impl FnMut(ReplayRow) -> Result<bool>,
) -> Result<()> {
    let mut readers = paths
        .iter()
        .map(|path| StrictRunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heads: Vec<Option<ReplayRow>> = (0..paths.len()).map(|_| None).collect();
    let mut heap = BinaryHeap::with_capacity(paths.len());
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(row) = reader.next()? {
            heap.push(Reverse((row.local_timestamp_ns, index)));
            heads[index] = Some(row);
        }
    }
    while let Some(Reverse((_, index))) = heap.pop() {
        if !emit(heads[index].take().expect("merge heap owns a head"))? {
            return Ok(());
        }
        if let Some(row) = readers[index].next()? {
            heap.push(Reverse((row.local_timestamp_ns, index)));
            heads[index] = Some(row);
        }
    }
    Ok(())
}

struct StrictSourceSlice {
    path: PathBuf,
    row_group: usize,
    rows: usize,
}

fn strict_file_groups(
    files: Vec<PathBuf>,
    start_ns: u64,
    end_ns: u64,
) -> Result<Vec<Vec<StrictSourceSlice>>> {
    let mut spans = Vec::new();
    for (file_index, path) in files.into_iter().enumerate() {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
        for (row_group, group) in builder.metadata().row_groups().iter().enumerate() {
            let (min, max) = row_group_receive_bounds(group).unwrap_or((0, u64::MAX));
            if max < start_ns || min >= end_ns || group.num_rows() == 0 {
                continue;
            }
            spans.push((
                min,
                max,
                (file_index, row_group),
                StrictSourceSlice {
                    path: path.clone(),
                    row_group,
                    rows: group.num_rows() as usize,
                },
            ));
        }
    }
    spans.sort_by_key(|entry| (entry.0, entry.2));
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut last_max = 0;
    for (min, max, index, slice) in spans {
        if !current.is_empty() && min > last_max {
            current.sort_by_key(|entry: &((usize, usize), StrictSourceSlice)| entry.0);
            groups.push(current.drain(..).map(|entry| entry.1).collect());
            last_max = 0;
        }
        last_max = last_max.max(max);
        current.push((index, slice));
    }
    if !current.is_empty() {
        current.sort_by_key(|entry| entry.0);
        groups.push(current.into_iter().map(|entry| entry.1).collect());
    }
    Ok(groups)
}

fn stream_strict_replay_files(
    files: Vec<PathBuf>,
    start_ns: u64,
    end_ns: u64,
    source: &str,
    options: ReplayOptions,
    batch_tx: &crossbeam_channel::Sender<std::result::Result<ReplayBatch, String>>,
    mut cache_writer: Option<ReplayCacheWriter>,
) -> Result<()> {
    let started = Instant::now();
    let groups = strict_file_groups(files, start_ns, end_ns)?;
    let group_count = groups.len();
    let mut scratch = StrictScratch::new()?;
    let mut loaded_rows = 0;
    let mut max_group_scratch_bytes = 0;
    let mut first_delivery_ms = None;
    for group in groups {
        // A disjoint row group fitting one bounded batch needs no scratch run.
        if group.len() == 1 && group[0].rows <= REPLAYER_BATCH_ROWS {
            let slice = &group[0];
            let mut consumer_open = true;
            stream_parquet_event_batches_selected(
                &slice.path,
                start_ns,
                end_ns,
                options.time_policy,
                Some(vec![slice.row_group]),
                |rows| {
                    first_delivery_ms.get_or_insert_with(|| started.elapsed().as_millis());
                    consumer_open = send_replay_rows(
                        batch_tx,
                        &slice.path,
                        &mut loaded_rows,
                        rows,
                        &mut cache_writer,
                    );
                    consumer_open
                },
            )?;
            if !consumer_open {
                return Ok(());
            }
            continue;
        }
        let mut runs = Vec::new();
        let mut group_scratch_bytes = 0;
        for slice in &group {
            let mut decode_error = None;
            stream_parquet_event_batches_selected(
                &slice.path,
                start_ns,
                end_ns,
                options.time_policy,
                Some(vec![slice.row_group]),
                |mut rows| {
                    rows.sort_by_key(|row| row.local_timestamp_ns);
                    let run_path = scratch.next_path();
                    let result = (|| -> Result<()> {
                        let mut writer =
                            BufWriter::with_capacity(64 * 1024, File::create(&run_path)?);
                        for row in &rows {
                            let bytes = write_strict_row(&mut writer, row)?;
                            group_scratch_bytes += bytes;
                            if group_scratch_bytes > STRICT_MAX_GROUP_RUN_BYTES {
                                return Err(anyhow!("strict receive overlap exceeds {} scratch bytes; shard tape by receive time or provide valid row-group receive bounds", STRICT_MAX_GROUP_RUN_BYTES));
                            }
                        }
                        writer.flush()?;
                        Ok(())
                    })();
                    if let Err(error) = result {
                        decode_error = Some(error);
                        return false;
                    }
                    runs.push(run_path);
                    true
                },
            )?;
            if let Some(error) = decode_error {
                return Err(error);
            }
        }
        max_group_scratch_bytes = max_group_scratch_bytes.max(group_scratch_bytes);
        while runs.len() > STRICT_MERGE_FAN_IN {
            let mut next = Vec::new();
            for chunk in runs.chunks(STRICT_MERGE_FAN_IN) {
                let path = scratch.next_path();
                let mut writer = BufWriter::with_capacity(64 * 1024, File::create(&path)?);
                merge_strict_runs(chunk, |row| {
                    write_strict_row(&mut writer, &row)?;
                    Ok(true)
                })?;
                writer.flush()?;
                drop(writer);
                for old in chunk {
                    std::fs::remove_file(old)?;
                }
                next.push(path);
            }
            runs = next;
        }
        let mut output = Vec::with_capacity(REPLAYER_BATCH_ROWS);
        let mut consumer_open = true;
        merge_strict_runs(&runs, |row| {
            output.push(row);
            if output.len() == REPLAYER_BATCH_ROWS {
                first_delivery_ms.get_or_insert_with(|| started.elapsed().as_millis());
                consumer_open = send_replay_rows(
                    batch_tx,
                    &scratch.directory,
                    &mut loaded_rows,
                    std::mem::replace(&mut output, Vec::with_capacity(REPLAYER_BATCH_ROWS)),
                    &mut cache_writer,
                );
            }
            Ok(consumer_open)
        })?;
        if consumer_open && !output.is_empty() {
            first_delivery_ms.get_or_insert_with(|| started.elapsed().as_millis());
            consumer_open = send_replay_rows(
                batch_tx,
                &scratch.directory,
                &mut loaded_rows,
                output,
                &mut cache_writer,
            );
        }
        if !consumer_open {
            return Ok(());
        }
        for path in runs {
            std::fs::remove_file(path)?;
        }
    }
    info!("[Replayer] ArrivalTimeStrict source={} groups={} rows={} first_delivery_ms={} max_group_run_bytes={} merge_fan_in={}",
        source, group_count, loaded_rows, first_delivery_ms.unwrap_or(0), max_group_scratch_bytes, STRICT_MERGE_FAN_IN);
    if let Some(writer) = cache_writer {
        writer.finish()?;
    }
    Ok(())
}

fn stream_replay_files(
    files: Vec<PathBuf>,
    start_ns: u64,
    end_ns: u64,
    source: &str,
    options: ReplayOptions,
    batch_tx: crossbeam_channel::Sender<std::result::Result<ReplayBatch, String>>,
    mut cache_writer: Option<ReplayCacheWriter>,
) {
    if options.time_policy == ReplayTimePolicy::ArrivalTimeStrict {
        if let Err(error) = stream_strict_replay_files(
            files,
            start_ns,
            end_ns,
            source,
            options,
            &batch_tx,
            cache_writer,
        ) {
            let _ = batch_tx.send(Err(format!("ArrivalTimeStrict source={source}: {error}")));
        }
        return;
    }
    let candidate_files = files.len();
    let mut loaded_files = 0_usize;
    let mut loaded_rows = 0_u64;
    for path in files {
        let mut consumer_open = true;
        let mut bootstrap_done = !options.bootstrap_binary_open;
        let mut bootstrap_rows = Vec::new();
        let result = stream_parquet_event_batches(
            &path,
            start_ns,
            end_ns,
            options.time_policy,
            |mut rows| {
                if rows.is_empty() {
                    return true;
                }
                if !bootstrap_done {
                    bootstrap_rows.append(&mut rows);
                    if binary_open_bootstrap_ready(&bootstrap_rows)
                        || bootstrap_rows.len() >= REPLAYER_BOOTSTRAP_MAX_ROWS
                    {
                        let mut combined = std::mem::take(&mut bootstrap_rows);
                        let repair = bootstrap_binary_event_open(&mut combined, options);
                        log_binary_open_repair(source, &path, repair);
                        bootstrap_done = true;
                        consumer_open = send_replay_rows(
                            &batch_tx,
                            &path,
                            &mut loaded_rows,
                            combined,
                            &mut cache_writer,
                        );
                    }
                    return consumer_open;
                }
                consumer_open =
                    send_replay_rows(&batch_tx, &path, &mut loaded_rows, rows, &mut cache_writer);
                consumer_open
            },
        );
        if consumer_open && !bootstrap_rows.is_empty() {
            let mut combined = std::mem::take(&mut bootstrap_rows);
            let repair = bootstrap_binary_event_open(&mut combined, options);
            log_binary_open_repair(source, &path, repair);
            consumer_open = send_replay_rows(
                &batch_tx,
                &path,
                &mut loaded_rows,
                combined,
                &mut cache_writer,
            );
        }
        match result {
            Ok(file_rows) => {
                if file_rows > 0 {
                    loaded_files = loaded_files.saturating_add(1);
                }
            }
            Err(error) => {
                let message = error.to_string();
                if message.starts_with("empty parquet file") {
                    warn!("[Replayer] Skip empty file {}: {}", path.display(), error);
                } else {
                    info!("[Replayer] Skip {}: {}", path.display(), error);
                }
            }
        }
        if !consumer_open {
            return;
        }
    }
    info!(
        "[Replayer] Replay source summary source={} candidate_files={} loaded_files={} loaded_rows={}",
        source, candidate_files, loaded_files, loaded_rows,
    );
    if let Some(writer) = cache_writer {
        let path = writer.final_path.clone();
        match writer.finish() {
            Ok(records) => info!(
                "[ReplayerCache] wrote source={} rows={} path={}",
                source,
                records,
                path.display(),
            ),
            Err(error) => warn!(
                "[ReplayerCache] finalize failed source={} path={}: {}",
                source,
                path.display(),
                error,
            ),
        }
    }
}

fn send_replay_rows(
    batch_tx: &crossbeam_channel::Sender<std::result::Result<ReplayBatch, String>>,
    path: &Path,
    loaded_rows: &mut u64,
    mut rows: Vec<ReplayRow>,
    cache_writer: &mut Option<ReplayCacheWriter>,
) -> bool {
    rows.sort_by_key(|row| row.local_timestamp_ns);
    if let Some(writer) = cache_writer.as_mut() {
        if let Err(error) = writer.write_rows(&rows) {
            warn!(
                "[ReplayerCache] write failed path={}: {}; disabling cache for this source",
                writer.final_path.display(),
                error,
            );
            *cache_writer = None;
        }
    }
    let count = rows.len() as u64;
    REPLAYER_LOADED_ROWS.fetch_add(count, Ordering::AcqRel);
    *loaded_rows = loaded_rows.saturating_add(count);
    log::debug!("[Replayer] Decoded {} rows from {}", count, path.display());
    batch_tx.send(Ok(ReplayBatch::new(rows))).is_ok()
}

fn log_binary_open_repair(source: &str, path: &Path, repair: BinaryOpenRepair) {
    if repair.seeded_books > 0 || repair.instrument_retimed {
        info!(
            "[Replayer] binary-open bootstrap source={} file={} event_start_ns={} instrument_retimed={} seeded_books={} max_original_gap_ms={:.3}",
            source,
            path.display(),
            repair.event_start_ns,
            repair.instrument_retimed,
            repair.seeded_books,
            repair.max_original_gap_ns as f64 / 1_000_000.0,
        );
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BinaryOpenRepair {
    event_start_ns: u64,
    instrument_retimed: bool,
    seeded_books: usize,
    max_original_gap_ns: u64,
}

fn binary_open_bootstrap_ready(rows: &[ReplayRow]) -> bool {
    let Some(tokens) = rows.iter().find_map(|row| match &row.event {
        MarketEvent::Instrument(Instrument::BinaryOption(option)) => Some(&option.clob_token_ids),
        _ => None,
    }) else {
        return false;
    };
    !tokens.is_empty()
        && tokens.iter().all(|token| {
            rows.iter().any(
                |row| matches!(&row.event, MarketEvent::OrderBook(book) if &book.symbol == token),
            )
        })
}

/// Repair the bounded opening window of one event parquet. Most events fit in
/// one row group; high-activity events may span two. Runtime never buffers more
/// than [`REPLAYER_BOOTSTRAP_MAX_ROWS`] for this startup-only repair.
fn bootstrap_binary_event_open(
    rows: &mut Vec<ReplayRow>,
    options: ReplayOptions,
) -> BinaryOpenRepair {
    if !options.bootstrap_binary_open || options.binary_open_max_backfill_ns == 0 {
        return BinaryOpenRepair::default();
    }

    let Some((instrument_index, event_start_ns, tokens)) =
        rows.iter()
            .enumerate()
            .find_map(|(index, row)| match &row.event {
                MarketEvent::Instrument(Instrument::BinaryOption(option)) => {
                    let start: u64 = chrono::DateTime::parse_from_rfc3339(&option.event_start_time)
                        .ok()?
                        .timestamp_nanos_opt()?
                        .try_into()
                        .ok()?;
                    Some((index, start, option.clob_token_ids.clone()))
                }
                _ => None,
            })
    else {
        return BinaryOpenRepair::default();
    };
    let bootstrap_ns = event_start_ns.saturating_add(options.binary_open_delay_ns);
    let mut repair = BinaryOpenRepair {
        event_start_ns,
        ..BinaryOpenRepair::default()
    };

    let instrument_ts = rows[instrument_index].local_timestamp_ns;
    let instrument_gap = instrument_ts.saturating_sub(bootstrap_ns);
    if instrument_ts > bootstrap_ns && instrument_gap <= options.binary_open_max_backfill_ns {
        rows[instrument_index].local_timestamp_ns = bootstrap_ns;
        repair.instrument_retimed = true;
        repair.max_original_gap_ns = instrument_gap;
    }

    let mut synthetic = Vec::with_capacity(tokens.len());
    for (token_index, token) in tokens.iter().enumerate() {
        let Some(book) = rows.iter().find_map(|row| match &row.event {
            MarketEvent::OrderBook(book) if &book.symbol == token => Some(book),
            _ => None,
        }) else {
            continue;
        };
        let original_ns = book.local_timestamp_ns.max(book.exchange_timestamp_ns);
        let book_ns = bootstrap_ns.saturating_add(1 + token_index as u64);
        let gap_ns = original_ns.saturating_sub(book_ns);
        if original_ns <= book_ns || gap_ns > options.binary_open_max_backfill_ns {
            continue;
        }
        let mut seeded = book.clone();
        seeded.exchange_timestamp_ns = book_ns;
        seeded.local_timestamp_ns = book_ns;
        synthetic.push(ReplayRow {
            local_timestamp_ns: book_ns,
            event: MarketEvent::OrderBook(seeded),
        });
        repair.seeded_books += 1;
        repair.max_original_gap_ns = repair.max_original_gap_ns.max(gap_ns);
    }
    rows.extend(synthetic);
    repair
}

/// Extract a Unix timestamp (seconds) and duration (seconds) from a parquet filename.
/// Returns (timestamp, duration_secs).
/// Handles: "btc-updown-5m-1774868400-321239.parquet" → Some((1774868400, 300))
///          "20260330_18.parquet" → Some((1774990800, 3600))
fn extract_file_timestamp(path: &Path) -> Option<(u64, u64)> {
    let raw_stem = path.file_stem()?.to_str()?;
    // Recorder row-group shards preserve the canonical base name and append
    // `.part-NNNNNN`; time filtering must use the base rather than interpreting
    // the shard number as an event id or timestamp.
    let stem = raw_stem
        .rsplit_once(".part-")
        .map(|(base, suffix)| {
            suffix
                .chars()
                .all(|character| character.is_ascii_digit())
                .then_some(base)
                .unwrap_or(raw_stem)
        })
        .unwrap_or(raw_stem);
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
                let duration = slug_prefix
                    .split('-')
                    .find_map(|p| {
                        p.strip_suffix('m')
                            .and_then(|n| n.parse::<u64>().ok().map(|n| n * 60))
                            .or_else(|| {
                                p.strip_suffix('h')
                                    .and_then(|n| n.parse::<u64>().ok().map(|n| n * 3600))
                            })
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
    files.sort_by_key(|f| {
        extract_file_timestamp(f)
            .map(|(ts, dur)| ts + dur)
            .unwrap_or(0)
    });
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
        return if skip_ws(b, i + 1) == n {
            Some(out)
        } else {
            None
        };
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

fn parse_market_event_json(json: Option<&str>) -> Option<MarketEvent> {
    serde_json::from_str::<MarketEvent>(json?).ok()
}

fn stream_parquet_event_batches(
    path: &Path,
    start_ns: u64,
    end_ns: u64,
    time_policy: ReplayTimePolicy,
    emit: impl FnMut(Vec<ReplayRow>) -> bool,
) -> Result<u64> {
    stream_parquet_event_batches_selected(path, start_ns, end_ns, time_policy, None, emit)
}

fn stream_parquet_event_batches_selected(
    path: &Path,
    start_ns: u64,
    end_ns: u64,
    time_policy: ReplayTimePolicy,
    row_groups: Option<Vec<usize>>,
    mut emit: impl FnMut(Vec<ReplayRow>) -> bool,
) -> Result<u64> {
    // Defensive size check before opening the parquet builder.
    // Zero-byte files appear at hour boundaries when the recorder
    // creates the file but crashes / restarts before writing any rows.
    // The arrow builder would still error ("Parquet file size is 0
    // bytes") but the typed error here surfaces the root cause cleanly
    // — and saves the syscall round-trip to mmap a footer that doesn't
    // exist.
    let md = std::fs::metadata(path).map_err(|e| anyhow!("metadata({}): {}", path.display(), e))?;
    if md.len() == 0 {
        return Err(anyhow!("empty parquet file ({})", path.display()));
    }
    let file = std::fs::File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(REPLAYER_BATCH_ROWS);
    let builder = if let Some(row_groups) = row_groups {
        builder.with_row_groups(row_groups)
    } else {
        builder
    };
    let reader = builder.build()?;

    let mut total_rows = 0_u64;
    let mut has_instrument = false;

    for batch_result in reader {
        let batch = batch_result?;
        let n = batch.num_rows();
        let mut rows = Vec::with_capacity(n.min(REPLAYER_BATCH_ROWS));

        let ts_col = batch
            .column_by_name("timestamp_ns")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let local_ts_col = batch
            .column_by_name("local_timestamp_ns")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let exchange_col = batch
            .column_by_name("exchange")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let etype_col = batch
            .column_by_name("event_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let symbol_col = batch
            .column_by_name("symbol")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let side_col = batch
            .column_by_name("side")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let price_col = batch
            .column_by_name("price")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let quantity_col = batch
            .column_by_name("quantity")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let bid_price_col = batch
            .column_by_name("bid_price")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ask_price_col = batch
            .column_by_name("ask_price")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let bid_qty_col = batch
            .column_by_name("bid_qty")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let ask_qty_col = batch
            .column_by_name("ask_qty")
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>());
        let bids_json_col = batch
            .column_by_name("bids_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let asks_json_col = batch
            .column_by_name("asks_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let data_json_col = batch
            .column_by_name("data_json")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let (ts_arr, local_ts_arr, exchange_arr, etype_arr, symbol_arr) =
            match (ts_col, local_ts_col, exchange_col, etype_col, symbol_col) {
                (Some(a), Some(b), Some(c), Some(d), Some(e)) => (a, b, c, d, e),
                _ if time_policy == ReplayTimePolicy::ArrivalTimeStrict => {
                    return Err(anyhow!(
                        "{}: missing required recorded timestamp/schema columns",
                        path.display()
                    ));
                }
                _ => continue, // legacy missing required columns
            };

        for i in 0..n {
            if time_policy == ReplayTimePolicy::ArrivalTimeStrict
                && (local_ts_arr.is_null(i) || local_ts_arr.value(i) == 0)
            {
                return Err(anyhow!(
                    "{}: missing/null/zero recorded receive timestamp at batch row {i}",
                    path.display()
                ));
            }
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
            let in_window = match time_policy {
                ReplayTimePolicy::LegacySourceTime => {
                    event_type == "instrument" || (exchange_ts >= start_ns && exchange_ts < end_ns)
                }
                ReplayTimePolicy::ArrivalTimeStrict => local_ts >= start_ns && local_ts < end_ns,
            };
            if !in_window {
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
                    let side_str =
                        side_col.and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
                    let side = match side_str {
                        Some("buy") => Side::Buy,
                        _ => Side::Sell,
                    };

                    MarketEvent::Trade(TradeTick {
                        exchange,
                        symbol: symbol.to_string(),
                        exchange_trade_id: None,
                        price,
                        quantity,
                        side,
                        exchange_timestamp_ns: exchange_ts,
                        local_timestamp_ns: local_ts,
                    })
                }
                "quote" => MarketEvent::Quote(QuoteTick {
                    exchange,
                    symbol: symbol.to_string(),
                    bid_price: bid_price_col.map(|c| c.value(i)).unwrap_or(0.0),
                    bid_qty: bid_qty_col.map(|c| c.value(i)).unwrap_or(0.0),
                    ask_price: ask_price_col.map(|c| c.value(i)).unwrap_or(0.0),
                    ask_qty: ask_qty_col.map(|c| c.value(i)).unwrap_or(0.0),
                    exchange_timestamp_ns: exchange_ts,
                    local_timestamp_ns: local_ts,
                }),
                "tick_size_change" => MarketEvent::TickSizeChange(TickSizeChange {
                    exchange,
                    symbol: symbol.to_string(),
                    old_tick_size: quantity_col.map(|c| c.value(i)).unwrap_or(0.0),
                    new_tick_size: price_col.map(|c| c.value(i)).unwrap_or(0.0),
                    exchange_timestamp_ns: exchange_ts,
                    local_timestamp_ns: local_ts,
                }),
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
                    let json_str =
                        data_json_col
                            .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
                    if let Some(event) = parse_market_event_json(json_str) {
                        event
                    } else {
                        continue;
                    }
                }
                "market_data_health" => {
                    let json_str =
                        data_json_col
                            .and_then(|c| if c.is_null(i) { None } else { Some(c.value(i)) });
                    let Some(event @ MarketEvent::MarketDataHealth(_)) =
                        parse_market_event_json(json_str)
                    else {
                        continue;
                    };
                    event
                }
                _ => continue,
            };

            rows.push(ReplayRow {
                local_timestamp_ns: local_ts,
                event,
            });
        }
        total_rows = total_rows.saturating_add(rows.len() as u64);
        if !rows.is_empty() && !emit(rows) {
            return Ok(total_rows);
        }
    }

    if !has_instrument && path.to_string_lossy().contains("polymarket") {
        warn!("[Replayer] No instrument in {}", path.display());
    }

    Ok(total_rows)
}

/// Compatibility helper for narrow metadata probes and tests. Runtime replay
/// uses [`stream_parquet_event_batches`] directly and never calls this full
/// collection path.
fn read_parquet_events(path: &Path, start_ns: u64, end_ns: u64) -> Result<Vec<ReplayRow>> {
    let mut rows = Vec::new();
    stream_parquet_event_batches(
        path,
        start_ns,
        end_ns,
        ReplayTimePolicy::LegacySourceTime,
        |batch| {
            rows.extend(batch);
            true
        },
    )?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_receive_fixture(
        path: &Path,
        entries: &[(u64, Option<u64>, f64)],
        row_group_rows: usize,
    ) {
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ns", DataType::UInt64, false),
            Field::new("local_timestamp_ns", DataType::UInt64, true),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("bid_price", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(
                    entries.iter().map(|e| e.0).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    entries.iter().map(|e| e.1).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec!["binance"; entries.len()])),
                Arc::new(StringArray::from(vec!["quote"; entries.len()])),
                Arc::new(StringArray::from(vec!["BTCUSDT"; entries.len()])),
                Arc::new(Float64Array::from(
                    entries.iter().map(|e| e.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_size(row_group_rows)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn receive_fixture_rows(
        directory: &Path,
        start_ns: u64,
        end_ns: u64,
    ) -> Result<Vec<(u64, u64, f64)>> {
        let mut replay = MarketReplayer::new_with_options(
            directory,
            "binance",
            "BTCUSDT",
            DateTime::from_timestamp_nanos(start_ns as i64),
            DateTime::from_timestamp_nanos(end_ns as i64),
            ReplayOptions {
                time_policy: ReplayTimePolicy::ArrivalTimeStrict,
                ..ReplayOptions::default()
            },
        )?;
        let mut rows = Vec::new();
        while let Some((receive, event)) = replay.next_event()? {
            let MarketEvent::Quote(quote) = event else {
                panic!("expected quote")
            };
            assert_eq!(receive, quote.local_timestamp_ns);
            rows.push((receive, quote.exchange_timestamp_ns, quote.bid_price));
        }
        Ok(rows)
    }

    #[test]
    fn arrival_strict_partitions_adjacent_windows_by_receive_across_source_files() {
        let directory = tempfile::tempdir().unwrap();
        let base = 1_789_307_400_000_000_000;
        let prior = directory.path().join("binance/BTCUSDT/20260913_12.parquet");
        let later = directory.path().join("binance/BTCUSDT/20260913_15.parquet");
        // Filename/source-time exclusion would miss both directions. Equal
        // receive messages and identical duplicate rows are legitimate inputs.
        write_receive_fixture(
            &prior,
            &[
                (base - 10, Some(base - 1), 1.0),
                (base - 9, Some(base), 2.0),
                (base - 9, Some(base), 2.0),
                (base - 8, Some(base + 1), 3.0),
            ],
            2,
        );
        write_receive_fixture(
            &later,
            &[
                (base + 10, Some(base - 2), 4.0),
                (base + 11, Some(base), 5.0),
                (base + 12, Some(base + 2), 6.0),
            ],
            2,
        );
        let warmup = receive_fixture_rows(directory.path(), base - 3, base).unwrap();
        let replay = receive_fixture_rows(directory.path(), base, base + 3).unwrap();
        let full = receive_fixture_rows(directory.path(), base - 3, base + 3).unwrap();
        assert_eq!(
            warmup,
            vec![(base - 2, base + 10, 4.0), (base - 1, base - 10, 1.0)]
        );
        assert_eq!(
            replay.iter().map(|r| r.2).collect::<Vec<_>>(),
            vec![2.0, 2.0, 5.0, 3.0, 6.0]
        );
        assert_eq!([warmup, replay].concat(), full);
        assert_eq!(full.len(), 7);
    }

    #[test]
    fn arrival_strict_merges_receive_regressions_across_batches_and_row_groups() {
        let directory = tempfile::tempdir().unwrap();
        let base = 1_789_307_400_000_000_000;
        let count = REPLAYER_BATCH_ROWS * 2 + 17;
        let entries = (0..count)
            .map(|index| {
                (
                    base + index as u64,
                    Some(base + (count - index) as u64),
                    index as f64,
                )
            })
            .collect::<Vec<_>>();
        write_receive_fixture(
            &directory.path().join("binance/BTCUSDT/20260913_13.parquet"),
            &entries,
            REPLAYER_BATCH_ROWS + 100,
        );
        let rows = receive_fixture_rows(directory.path(), base, base + count as u64 + 1).unwrap();
        assert_eq!(rows.len(), count);
        assert!(rows.windows(2).all(|pair| pair[0].0 <= pair[1].0));
        assert_eq!(
            rows[0],
            (base + 1, base + count as u64 - 1, (count - 1) as f64)
        );
        assert_eq!(rows[count - 1], (base + count as u64, base, 0.0));
    }

    #[test]
    fn arrival_strict_rejects_missing_receive_provenance_and_future_bootstrap() {
        let base = 1_789_307_400_000_000_000;
        for receive in [None, Some(0)] {
            let directory = tempfile::tempdir().unwrap();
            write_receive_fixture(
                &directory.path().join("binance/BTCUSDT/20260913_13.parquet"),
                &[(base, receive, 1.0)],
                REPLAYER_BATCH_ROWS,
            );
            let error = receive_fixture_rows(directory.path(), base, base + 10).unwrap_err();
            assert!(
                error.to_string().contains("recorded receive timestamp"),
                "{error}"
            );
        }
        let error = ReplayOptions {
            time_policy: ReplayTimePolicy::ArrivalTimeStrict,
            bootstrap_binary_open: true,
            ..ReplayOptions::default()
        }
        .validate()
        .unwrap_err();
        assert!(error.to_string().contains("future binary-open bootstrap"));
        ReplayOptions::default().validate().unwrap();
    }

    #[test]
    fn arrival_strict_stable_ties_survive_multiple_merge_passes() {
        let directory = tempfile::tempdir().unwrap();
        let base = 1_789_307_400_000_000_000;
        let count = (STRICT_MERGE_FAN_IN + 1) * 128;
        let entries = (0..count)
            .map(|index| (base - index as u64, Some(base), index as f64))
            .collect::<Vec<_>>();
        write_receive_fixture(
            &directory.path().join("binance/BTCUSDT/20260913_13.parquet"),
            &entries,
            128,
        );
        let rows = receive_fixture_rows(directory.path(), base, base + 1).unwrap();
        assert_eq!(rows.len(), count);
        for (index, row) in rows.iter().enumerate() {
            assert_eq!(*row, (base, base - index as u64, index as f64));
        }
    }

    #[test]
    fn arrival_strict_instruments_obey_the_same_receive_cutoff() {
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("instruments.parquet");
        let base = 1_789_307_400_000_000_000;
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp_ns", DataType::UInt64, false),
            Field::new("local_timestamp_ns", DataType::UInt64, false),
            Field::new("exchange", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("symbol", DataType::Utf8, false),
            Field::new("data_json", DataType::Utf8, false),
        ]));
        let json = serde_json::to_string(&MarketEvent::Instrument(binary_option(
            "2026-09-13T13:50:00Z",
        )))
        .unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![base - 10; 3])),
                Arc::new(UInt64Array::from(vec![base - 1, base, base + 1])),
                Arc::new(StringArray::from(vec!["polymarket"; 3])),
                Arc::new(StringArray::from(vec!["instrument"; 3])),
                Arc::new(StringArray::from(vec!["up"; 3])),
                Arc::new(StringArray::from(vec![json.as_str(); 3])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut times = Vec::new();
        for (start, end) in [(base - 1, base), (base, base + 2)] {
            let mut window = Vec::new();
            stream_parquet_event_batches(
                &path,
                start,
                end,
                ReplayTimePolicy::ArrivalTimeStrict,
                |rows| {
                    for row in rows {
                        assert!(matches!(row.event, MarketEvent::Instrument(_)));
                        window.push(row.local_timestamp_ns);
                    }
                    true
                },
            )
            .unwrap();
            times.push(window);
        }
        assert_eq!(times, vec![vec![base - 1], vec![base, base + 1]]);
    }

    #[test]
    fn arrival_strict_missing_receive_column_is_an_error_not_an_empty_stream() {
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing_receive.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ns",
            DataType::UInt64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![100]))])
                .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let error = stream_parquet_event_batches(
            &path,
            1,
            200,
            ReplayTimePolicy::ArrivalTimeStrict,
            |_| true,
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("missing required recorded timestamp/schema columns"));
    }

    #[test]
    fn arrival_strict_cache_cannot_hit_a_legacy_source_filtered_entry() {
        let files = Vec::new();
        let legacy = replay_cache_fingerprint(
            &files,
            "binance/BTCUSDT",
            100,
            200,
            ReplayOptions::default(),
        );
        let strict = replay_cache_fingerprint(
            &files,
            "binance/BTCUSDT",
            100,
            200,
            ReplayOptions {
                time_policy: ReplayTimePolicy::ArrivalTimeStrict,
                ..ReplayOptions::default()
            },
        );
        assert_ne!(legacy, strict);
    }

    fn binary_option(start: &str) -> Instrument {
        Instrument::BinaryOption(BinaryOption {
            exchange: Exchange::Polymarket,
            id: "event-1".to_string(),
            question: String::new(),
            condition_id: "condition-1".to_string(),
            series_slug: "btc-up-or-down-5m".to_string(),
            slug: "btc-updown-5m-1774868400".to_string(),
            clob_token_ids: vec!["up".to_string(), "down".to_string()],
            outcomes: vec!["Up".to_string(), "Down".to_string()],
            outcome_prices: Vec::new(),
            active: true,
            closed: false,
            volume: 0.0,
            liquidity: 0.0,
            tick_size: 0.01,
            order_min_size: 5.0,
            group_item_title: String::new(),
            event_start_time: start.to_string(),
            base_fee: 0,
            fee_exponent: 0.0,
            fee_rate: 0.0,
            fee_settlement: Default::default(),
        })
    }

    fn book(symbol: &str, timestamp_ns: u64) -> OrderBookSnapshot {
        OrderBookSnapshot {
            exchange: Exchange::Polymarket,
            symbol: symbol.to_string(),
            bids: vec![PriceLevel {
                price: 0.49,
                quantity: 100.0,
            }],
            asks: vec![PriceLevel {
                price: 0.51,
                quantity: 100.0,
            }],
            exchange_timestamp_ns: timestamp_ns,
            local_timestamp_ns: timestamp_ns,
        }
    }

    #[test]
    fn binary_open_bootstrap_retimes_instrument_and_seeds_each_token_book() {
        let event_start_ns = 1_774_868_400_u64 * 1_000_000_000;
        let mut rows = vec![
            ReplayRow {
                local_timestamp_ns: event_start_ns + 1_700_000_000,
                event: MarketEvent::Instrument(binary_option("2026-03-30T11:00:00Z")),
            },
            ReplayRow {
                local_timestamp_ns: event_start_ns + 2_300_000_000,
                event: MarketEvent::OrderBook(book("up", event_start_ns + 2_300_000_000)),
            },
            ReplayRow {
                local_timestamp_ns: event_start_ns + 2_500_000_000,
                event: MarketEvent::OrderBook(book("down", event_start_ns + 2_500_000_000)),
            },
        ];
        let options = ReplayOptions {
            bootstrap_binary_open: true,
            binary_open_delay_ns: 20_000_000,
            binary_open_max_backfill_ns: 5_000_000_000,
            ..ReplayOptions::default()
        };

        let repair = bootstrap_binary_event_open(&mut rows, options);
        rows.sort_by_key(|row| row.local_timestamp_ns);

        assert!(repair.instrument_retimed);
        assert_eq!(repair.seeded_books, 2);
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].local_timestamp_ns, event_start_ns + 20_000_000);
        assert!(matches!(rows[0].event, MarketEvent::Instrument(_)));
        for (index, token) in ["up", "down"].iter().enumerate() {
            let expected = event_start_ns + 20_000_001 + index as u64;
            let seeded = rows.iter().find_map(|row| match &row.event {
                MarketEvent::OrderBook(book)
                    if book.symbol == *token && row.local_timestamp_ns == expected =>
                {
                    Some(book)
                }
                _ => None,
            });
            let seeded = seeded.expect("seeded book");
            assert_eq!(seeded.exchange_timestamp_ns, expected);
            assert_eq!(seeded.local_timestamp_ns, expected);
        }
    }

    #[test]
    fn binary_open_bootstrap_refuses_unbounded_future_book() {
        let event_start_ns = 1_774_868_400_u64 * 1_000_000_000;
        let mut rows = vec![
            ReplayRow {
                local_timestamp_ns: event_start_ns + 9_000_000_000,
                event: MarketEvent::Instrument(binary_option("2026-03-30T11:00:00Z")),
            },
            ReplayRow {
                local_timestamp_ns: event_start_ns + 9_000_000_000,
                event: MarketEvent::OrderBook(book("up", event_start_ns + 9_000_000_000)),
            },
        ];
        let repair = bootstrap_binary_event_open(
            &mut rows,
            ReplayOptions {
                bootstrap_binary_open: true,
                binary_open_delay_ns: 20_000_000,
                binary_open_max_backfill_ns: 5_000_000_000,
                ..ReplayOptions::default()
            },
        );
        assert!(!repair.instrument_retimed);
        assert_eq!(repair.seeded_books, 0);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn binary_open_bootstrap_waits_for_both_token_books_across_batches() {
        let event_start_ns = 1_774_868_400_u64 * 1_000_000_000;
        let mut rows = vec![ReplayRow {
            local_timestamp_ns: event_start_ns + 1_700_000_000,
            event: MarketEvent::Instrument(binary_option("2026-03-30T11:00:00Z")),
        }];
        assert!(!binary_open_bootstrap_ready(&rows));
        rows.push(ReplayRow {
            local_timestamp_ns: event_start_ns + 2_300_000_000,
            event: MarketEvent::OrderBook(book("up", event_start_ns + 2_300_000_000)),
        });
        assert!(!binary_open_bootstrap_ready(&rows));
        rows.push(ReplayRow {
            local_timestamp_ns: event_start_ns + 2_500_000_000,
            event: MarketEvent::OrderBook(book("down", event_start_ns + 2_500_000_000)),
        });
        assert!(binary_open_bootstrap_ready(&rows));
    }

    #[test]
    fn shard_suffix_keeps_base_time_window() {
        let hourly = Path::new("20260823_14.part-000042.parquet");
        let five_minute = Path::new("20260823_1430.part-000007.parquet");
        let slug = Path::new("btc-updown-5m-1774868400-321239.part-000003.parquet");

        assert_eq!(
            extract_file_timestamp(hourly).map(|value| value.1),
            Some(3600)
        );
        assert_eq!(
            extract_file_timestamp(five_minute).map(|value| value.1),
            Some(300)
        );
        assert_eq!(extract_file_timestamp(slug), Some((1_774_868_400, 300)));
    }

    #[test]
    fn market_replayer_streams_multiple_shards_through_two_batch_window() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let mut recorder =
            crate::recorder::MarketRecorder::new(tempdir.path().to_path_buf()).expect("recorder");
        let base_ns = 1_774_868_400_u64 * 1_000_000_000;
        for index in 0..20_000_u64 {
            recorder
                .write_event(&MarketEvent::Quote(QuoteTick {
                    exchange: Exchange::Binance,
                    symbol: "BTCUSDT".to_string(),
                    bid_price: 100.0,
                    bid_qty: 1.0,
                    ask_price: 101.0,
                    ask_qty: 1.0,
                    exchange_timestamp_ns: base_ns + index,
                    local_timestamp_ns: base_ns + index,
                }))
                .expect("record quote");
        }
        recorder.flush().expect("flush recorder");

        let start = DateTime::<Utc>::from_timestamp((base_ns / 1_000_000_000) as i64, 0).unwrap();
        let end = start + chrono::Duration::hours(1);
        let mut replay = MarketReplayer::new(tempdir.path(), "binance", "BTCUSDT", start, end)
            .expect("replayer");

        let mut count = 0_u64;
        let mut previous = 0_u64;
        while let Some((timestamp, event)) = replay.next_event().expect("next event") {
            assert!(matches!(event, MarketEvent::Quote(_)));
            assert!(timestamp >= previous);
            previous = timestamp;
            count += 1;
            let resident_capacity = replay
                .current_batch
                .as_ref()
                .map(|batch| batch.rows.capacity())
                .unwrap_or(0)
                + replay
                    .lookahead_batch
                    .as_ref()
                    .map(|batch| batch.rows.capacity())
                    .unwrap_or(0);
            assert!(resident_capacity <= REPLAYER_BATCH_ROWS * 2);
        }
        assert_eq!(count, 20_000);
    }

    #[test]
    fn market_data_health_json_roundtrips_for_replay() {
        let event = MarketEvent::MarketDataHealth(crate::types::MarketDataHealth {
            exchange: Exchange::Polymarket,
            market_id: "condition".to_string(),
            symbol: "up-token".to_string(),
            state: crate::types::MarketDataHealthState::Repairing,
            passive_ready: true,
            taker_ready: false,
            reason: "repair in progress".to_string(),
            local_timestamp_ns: 456,
        });
        let json = serde_json::to_string(&event).unwrap();
        let decoded = parse_market_event_json(Some(&json)).expect("decode health event");
        let MarketEvent::MarketDataHealth(health) = decoded else {
            panic!("decoded wrong event variant")
        };
        assert_eq!(health.market_id, "condition");
        assert_eq!(health.state, crate::types::MarketDataHealthState::Repairing);
        assert_eq!(health.local_timestamp_ns, 456);
    }

    #[test]
    fn market_data_health_csv_replays_in_timestamp_order_and_filters_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.csv");
        std::fs::write(
            &path,
            concat!(
                "timestamp_ns,market_id,state,passive_ready,taker_ready,reason\n",
                "3000000000,cid,Healthy,true,true,recovered\n",
                "1000000000,cid,Settling,true,false,bbo edge\n",
                "5000000000,cid,Degraded,false,false,outside window\n",
            ),
        )
        .unwrap();
        let start = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
        let end = DateTime::<Utc>::from_timestamp(4, 0).unwrap();
        let mut replay = MarketReplayer::from_market_data_health_csv(&path, start, end).unwrap();
        let (first_ts, first) = replay.next_event().unwrap().unwrap();
        let (second_ts, second) = replay.next_event().unwrap().unwrap();
        assert_eq!(first_ts, 1_000_000_000);
        assert_eq!(second_ts, 3_000_000_000);
        let MarketEvent::MarketDataHealth(first) = first else {
            panic!("expected health event")
        };
        let MarketEvent::MarketDataHealth(second) = second else {
            panic!("expected health event")
        };
        assert_eq!(first.state, MarketDataHealthState::Settling);
        assert!(!first.taker_ready);
        assert_eq!(second.state, MarketDataHealthState::Healthy);
        assert!(second.taker_ready);
        assert!(replay.next_event().unwrap().is_none());
    }

    /// The custom order-book parser must produce **bit-identical** `f64` output
    /// to `serde_json::from_str::<Vec<PriceLevel>>` for every input — otherwise
    /// the parse change would alter backtest fills. Compares via `to_bits()`.
    #[test]
    fn custom_parser_matches_serde_json() {
        let cases = [
            r#"[{"price":0.52,"quantity":100.0},{"price":0.51,"quantity":250.5}]"#,
            r#"[{"price":0.999,"quantity":1.0},{"price":0.001,"quantity":1000000.0}]"#,
            r#"[{"price":1,"quantity":0}]"#, // integer literals
            r#"[{"price":6.1e-2,"quantity":1.25e3}]"#, // scientific notation
            r#"[{"price":-0.0,"quantity":12.5}]"#, // signed zero
            r#"[]"#,                         // empty book side
            r#"[{"price":0.6612345678901234,"quantity":0.1}]"#, // long mantissa (rounding)
            r#" [ {"price": 0.5 , "quantity": 3.0 } ] "#, // whitespace → fallback path
            r#"[{"quantity":3.0,"price":0.5}]"#, // field order reversed → fallback
            r#"not json"#,                   // malformed → empty
            r#"[{"price":0.5}]"#,            // missing field → fallback→empty
        ];
        for c in cases {
            let got = parse_price_levels(c);
            let serde = serde_json::from_str::<Vec<PriceLevel>>(c).unwrap_or_default();
            assert_eq!(got.len(), serde.len(), "len mismatch for {c}");
            for (a, b) in got.iter().zip(serde.iter()) {
                assert_eq!(
                    a.price.to_bits(),
                    b.price.to_bits(),
                    "price bits differ for {c}"
                );
                assert_eq!(
                    a.quantity.to_bits(),
                    b.quantity.to_bits(),
                    "qty bits differ for {c}"
                );
            }
        }
    }

    /// The EXACT recorder path: values serialised by `serde_json::to_string`
    /// (what `writer.rs` writes) must parse back bit-identically vs serde_json.
    #[test]
    fn custom_parser_roundtrips_recorder_format() {
        let books: Vec<Vec<PriceLevel>> = vec![
            vec![
                PriceLevel {
                    price: 0.523,
                    quantity: 100.0,
                },
                PriceLevel {
                    price: 0.517,
                    quantity: 250.5,
                },
            ],
            vec![
                PriceLevel {
                    price: 0.0001,
                    quantity: 1_000_000.0,
                },
                PriceLevel {
                    price: 0.9999,
                    quantity: 0.01,
                },
            ],
            vec![PriceLevel {
                price: 1.0 / 3.0,
                quantity: 7.0 / 11.0,
            }], // non-terminating decimals
            vec![],
        ];
        for b in &books {
            let s = serde_json::to_string(b).unwrap(); // recorder's exact output
            let got = parse_price_levels(&s);
            let serde = serde_json::from_str::<Vec<PriceLevel>>(&s).unwrap_or_default();
            assert_eq!(got.len(), serde.len(), "len mismatch for {s}");
            for (x, y) in got.iter().zip(serde.iter()) {
                assert_eq!(
                    x.price.to_bits(),
                    y.price.to_bits(),
                    "price bits differ for {s}"
                );
                assert_eq!(
                    x.quantity.to_bits(),
                    y.quantity.to_bits(),
                    "qty bits differ for {s}"
                );
                // and equal to the ORIGINAL f64 (round-trip)
            }
            for (x, orig) in got.iter().zip(b.iter()) {
                assert_eq!(
                    x.price.to_bits(),
                    orig.price.to_bits(),
                    "price not round-tripped for {s}"
                );
                assert_eq!(
                    x.quantity.to_bits(),
                    orig.quantity.to_bits(),
                    "qty not round-tripped for {s}"
                );
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
        std::fs::File::create(&path).unwrap(); // 0 bytes
        let err = match read_parquet_events(&path, 0, u64::MAX) {
            Err(e) => e,
            Ok(_) => panic!("must error on zero-byte parquet"),
        };
        let msg = err.to_string();
        assert!(
            msg.starts_with("empty parquet file"),
            "expected `empty parquet file ...`, got: {}",
            msg,
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn binary_replay_cache_roundtrips_events_and_rejects_foreign_fingerprint() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.replay.bin");
        let fingerprint = [0x4d; 32];
        let rows = vec![
            ReplayRow {
                local_timestamp_ns: 101,
                event: MarketEvent::Quote(QuoteTick {
                    exchange: Exchange::Binance,
                    symbol: "BTCUSDT".to_string(),
                    bid_price: 70_000.25,
                    bid_qty: 1.5,
                    ask_price: 70_000.5,
                    ask_qty: 2.5,
                    exchange_timestamp_ns: 99,
                    local_timestamp_ns: 101,
                }),
            },
            ReplayRow {
                local_timestamp_ns: 205,
                event: MarketEvent::SpotPrice(SpotPrice {
                    source: "chainlink".to_string(),
                    symbol: "BTC/USD".to_string(),
                    price: 70_001.125,
                    timestamp_ns: 200,
                    local_timestamp_ns: 205,
                }),
            },
        ];
        let expected: Vec<Vec<u8>> = rows
            .iter()
            .map(|row| rmp_serde::to_vec(&row.event).unwrap())
            .collect();
        let mut writer = ReplayCacheWriter::create(path.clone(), fingerprint).unwrap();
        writer.write_rows(&rows).unwrap();
        assert_eq!(writer.finish().unwrap(), 2);
        assert!(replay_cache_header_matches(&path, fingerprint));
        assert!(!replay_cache_header_matches(&path, [0x5e; 32]));

        let (tx, rx) = crossbeam_channel::unbounded();
        assert_eq!(stream_replay_cache(&path, fingerprint, &tx).unwrap(), 2);
        drop(tx);
        let batches: Vec<ReplayBatch> = rx.into_iter().map(|batch| batch.unwrap()).collect();
        let decoded: Vec<&ReplayRow> = batches.iter().flat_map(|batch| batch.rows.iter()).collect();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].local_timestamp_ns, 101);
        assert_eq!(decoded[1].local_timestamp_ns, 205);
        for (actual, expected) in decoded.into_iter().zip(expected) {
            assert_eq!(rmp_serde::to_vec(&actual.event).unwrap(), expected);
        }
        assert!(stream_replay_cache(&path, [0x5e; 32], &crossbeam_channel::unbounded().0).is_err());
    }

    #[test]
    fn binary_replay_cache_accepts_maximum_repaired_bootstrap_batch() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("maximum.replay.bin");
        let fingerprint = [0x6a; 32];
        let rows: Vec<ReplayRow> = (0..REPLAY_CACHE_MAX_BATCH_ROWS)
            .map(|index| ReplayRow {
                local_timestamp_ns: index as u64,
                event: MarketEvent::Quote(QuoteTick {
                    exchange: Exchange::Binance,
                    symbol: "BTCUSDT".to_string(),
                    bid_price: 70_000.25,
                    bid_qty: 1.5,
                    ask_price: 70_000.5,
                    ask_qty: 2.5,
                    exchange_timestamp_ns: index as u64,
                    local_timestamp_ns: index as u64,
                }),
            })
            .collect();
        let mut writer = ReplayCacheWriter::create(path.clone(), fingerprint).unwrap();
        writer.write_rows(&rows).unwrap();
        assert_eq!(writer.finish().unwrap(), REPLAY_CACHE_MAX_BATCH_ROWS as u64);

        let (tx, rx) = crossbeam_channel::unbounded();
        assert_eq!(
            stream_replay_cache(&path, fingerprint, &tx).unwrap(),
            REPLAY_CACHE_MAX_BATCH_ROWS as u64
        );
        drop(tx);
        let batch = rx.recv().unwrap().unwrap();
        assert_eq!(batch.rows.len(), REPLAY_CACHE_MAX_BATCH_ROWS);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn binary_replay_cache_fingerprint_tracks_source_metadata_and_options() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.parquet");
        std::fs::write(&path, b"first").unwrap();
        let files = vec![path.clone()];
        let baseline = replay_cache_fingerprint(
            &files,
            "binance/BTCUSDT",
            100,
            200,
            ReplayOptions::default(),
        );
        let different_window = replay_cache_fingerprint(
            &files,
            "binance/BTCUSDT",
            100,
            201,
            ReplayOptions::default(),
        );
        assert_ne!(baseline, different_window);
        std::fs::write(&path, b"second-longer").unwrap();
        let changed_file = replay_cache_fingerprint(
            &files,
            "binance/BTCUSDT",
            100,
            200,
            ReplayOptions::default(),
        );
        assert_ne!(baseline, changed_file);
        let changed_repair = replay_cache_fingerprint(
            &files,
            "binance/BTCUSDT",
            100,
            200,
            ReplayOptions {
                bootstrap_binary_open: true,
                binary_open_delay_ns: 20_000_000,
                binary_open_max_backfill_ns: 5_000_000_000,
                ..ReplayOptions::default()
            },
        );
        assert_ne!(changed_file, changed_repair);
    }

    #[test]
    fn unfinished_binary_replay_cache_is_not_installed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.replay.bin");
        let temp_path = {
            let writer = ReplayCacheWriter::create(path.clone(), [0x7f; 32]).unwrap();
            let temp = writer.temp_path.clone();
            assert!(temp.exists());
            temp
        };
        assert!(!temp_path.exists());
        assert!(!path.exists());
    }

    #[test]
    fn replay_cache_loader_error_is_not_reported_as_clean_eof() {
        let (tx, rx) = crossbeam_channel::unbounded();
        tx.send(Err("synthetic corrupt replay cache".to_string()))
            .unwrap();
        drop(tx);
        REPLAYER_ACTIVE_STREAMS.fetch_add(1, Ordering::AcqRel);
        let mut replayer = MarketReplayer {
            batch_rx: Some(rx),
            loader_handle: None,
            current_batch: None,
            lookahead_batch: None,
            row_cursor: 0,
            event_count: 0,
            source: "cache-error-test".to_string(),
        };
        let error = replayer.next_event().unwrap_err();
        assert!(error.to_string().contains("synthetic corrupt replay cache"));
    }
}
