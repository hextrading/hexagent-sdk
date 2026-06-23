//! Per-request place/cancel latency recorder.
//!
//! Captures EACH individual `POST /order(s)` (place) and `DELETE
//! /order(s) | /cancel-all` (cancel) round-trip latency to a CSV file —
//! covering BOTH normal trading (real quotes routed through the
//! executor) and the RTT-probe task's synthetic place/cancel cycles,
//! since both flow through the same `SharedState::http_call_async`
//! choke point where the recording happens.
//!
//! ## Activation
//!
//! Recording is a process-global singleton (mirrors `latency.rs`),
//! installed once via [`init`]. The engine installs it in live mode when
//! either:
//!   * `[general] latency_record_enabled = true` — log latencies during
//!     normal trading, or
//!   * `[general] all_probe = true` — the no-trading probe session
//!     (which implies recording).
//!
//! When not installed, [`record`] / [`maybe_flush`] / [`flush`] are
//! cheap no-ops (a single `OnceLock` load), so the call sites stay on
//! the hot path unconditionally.
//!
//! ## File layout
//!
//!   * One file per **UTC day**, rotated at 00:00 UTC:
//!     `<latency_record>/<YYYYMMDD>.csv`, where the date is
//!     each row's own UTC calendar date. Runs that span midnight — or
//!     several runs within one UTC day — append into the matching daily
//!     file, and a flush whose buffer straddles 00:00 UTC routes each row
//!     to its own day's file (so the boundary is exact, not "whichever
//!     day the flush happened to fire").
//!   * Rows are buffered in memory and appended to disk **every 5
//!     minutes, aligned to the wall clock** (shortly after each
//!     `:00 / :05 / :10 …` boundary — 00:00 UTC is one such boundary, so
//!     the day rolls promptly), plus a final flush on shutdown.
//!   * Columns: `epoch_ms,iso_local,instance_id,kind,rtt_ms,status` —
//!     `kind` is `place` or `cancel`; `status` is `ok`, `timeout`,
//!     `http_<code>`, or `error`.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use log::{info, warn};

/// Flush cadence in seconds — 5 minutes. Bucketing on
/// `epoch_secs / FLUSH_BUCKET_SECS` aligns every flush to a wall-clock
/// 5-minute boundary (and, for any timezone whose UTC offset is a
/// multiple of 5 minutes — i.e. every standard zone — to a *local*
/// 5-minute boundary too).
const FLUSH_BUCKET_SECS: u64 = 300;

static RECORDER: OnceLock<LatencyRecorder> = OnceLock::new();

/// Install the process-global recorder writing daily files under `<dir>`
/// named `<UTC-YYYYMMDD>.csv` (rotated at 00:00 UTC).
/// `start_label` is the run-start timestamp, kept only for the startup
/// log line. Idempotent — the first call wins; later calls are ignored.
/// Returns `true` iff this call installed it.
pub fn init(dir: &str, start_label: &str) -> bool {
    RECORDER.set(LatencyRecorder::new(dir, start_label)).is_ok()
}

/// True once [`init`] has installed the recorder.
#[inline]
pub fn is_active() -> bool {
    RECORDER.get().is_some()
}

/// Buffer one place/cancel latency sample. No-op when recording is off.
#[inline]
pub fn record(instance_id: &str, kind: &'static str, rtt_ms: f64, status: String) {
    if let Some(r) = RECORDER.get() {
        r.record(instance_id, kind, rtt_ms, status);
    }
}

/// Flush iff the wall clock has crossed into a new 5-minute bucket.
/// No-op when recording is off. Cheap to call on a poll loop.
pub fn maybe_flush() {
    if let Some(r) = RECORDER.get() {
        r.maybe_flush();
    }
}

/// Force-flush any buffered rows (used on shutdown). No-op when off.
pub fn flush() {
    if let Some(r) = RECORDER.get() {
        r.flush();
    }
}

/// One latency observation awaiting flush.
struct Row {
    epoch_ms: u64,
    instance_id: String,
    /// `"place"` or `"cancel"` — `&'static str`, no allocation.
    kind: &'static str,
    rtt_ms: f64,
    /// `"ok"` / `"timeout"` / `"http_<code>"` / `"error"`.
    status: String,
}

struct LatencyRecorder {
    /// Output directory. The actual file is chosen per flush from each
    /// row's UTC date (`<YYYYMMDD>.csv`), so output rolls
    /// to a fresh file at 00:00 UTC.
    dir: PathBuf,
    buf: Mutex<Vec<Row>>,
    /// Wall-clock 5-min bucket index (`epoch_secs / 300`) of the last
    /// flush. Seeded to the current bucket at construction so the first
    /// flush happens at the next boundary, not immediately.
    last_flush_bucket: AtomicU64,
}

impl LatencyRecorder {
    /// Create a recorder writing daily files under `<dir>` named
    /// `<UTC-YYYYMMDD>.csv` (rotated at 00:00 UTC).
    /// `start_label` is the run start timestamp (e.g. `20260614_143052`),
    /// used only for the startup log line. The directory is created if
    /// missing.
    fn new(dir: &str, start_label: &str) -> Self {
        let dir_path = PathBuf::from(dir);
        if let Err(e) = fs::create_dir_all(&dir_path) {
            warn!(
                "[LatencyRecorder] create_dir_all({}) failed: {} — latency records may not persist",
                dir_path.display(), e,
            );
        }
        info!(
            "[LatencyRecorder] per-request place/cancel latency → {}/<UTC-date>.csv \
             (run started {}, daily UTC rotation, flush every {}s aligned to wall clock)",
            dir_path.display(), start_label, FLUSH_BUCKET_SECS,
        );
        Self {
            dir: dir_path,
            buf: Mutex::new(Vec::with_capacity(1024)),
            last_flush_bucket: AtomicU64::new(current_bucket()),
        }
    }

    fn record(&self, instance_id: &str, kind: &'static str, rtt_ms: f64, status: String) {
        let epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        if let Ok(mut buf) = self.buf.lock() {
            buf.push(Row {
                epoch_ms,
                instance_id: instance_id.to_string(),
                kind,
                rtt_ms,
                status,
            });
        }
    }

    /// Flush iff a new 5-minute bucket has begun. The CAS makes the
    /// boundary flush fire exactly once even when several instances'
    /// threads race here.
    fn maybe_flush(&self) {
        let bucket = current_bucket();
        let last = self.last_flush_bucket.load(Ordering::Relaxed);
        if bucket > last
            && self
                .last_flush_bucket
                .compare_exchange(last, bucket, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.flush();
        }
    }

    /// Append buffered rows to per-UTC-day CSV files (creating each file +
    /// its header on first write). Rows are routed by their own UTC date,
    /// so a buffer that straddles 00:00 UTC splits cleanly across two
    /// daily files. A no-op when the buffer is empty.
    fn flush(&self) {
        let rows: Vec<Row> = match self.buf.lock() {
            Ok(mut b) => std::mem::take(&mut *b),
            Err(_) => return,
        };
        if rows.is_empty() {
            return;
        }
        // Group by UTC date (`YYYYMMDD`), preserving each day's row order.
        // BTreeMap keeps the (rare) midnight-straddling two-day split
        // deterministic (older day first).
        let mut by_date: std::collections::BTreeMap<String, Vec<&Row>> =
            std::collections::BTreeMap::new();
        for r in &rows {
            by_date.entry(utc_date(r.epoch_ms)).or_default().push(r);
        }
        for (date, day_rows) in by_date {
            let path = self.dir.join(format!("{}.csv", date));
            let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(f) => f,
                Err(e) => {
                    warn!(
                        "[LatencyRecorder] open {} failed: {} — dropping {} rows",
                        path.display(), e, day_rows.len(),
                    );
                    continue;
                }
            };
            // Header only when the file is brand-new / empty — covers both
            // the first write of a fresh day and resuming a day's file
            // after a process restart (no spurious mid-file header).
            let need_header = file.metadata().map(|m| m.len() == 0).unwrap_or(false);
            let mut out = String::with_capacity(day_rows.len() * 80 + 64);
            if need_header {
                out.push_str("epoch_ms,iso_local,instance_id,kind,rtt_ms,status\n");
            }
            for r in &day_rows {
                out.push_str(&format!(
                    "{},{},{},{},{:.3},{}\n",
                    r.epoch_ms,
                    format_local(r.epoch_ms),
                    r.instance_id,
                    r.kind,
                    r.rtt_ms,
                    r.status,
                ));
            }
            match file.write_all(out.as_bytes()) {
                Ok(()) => info!(
                    "[LatencyRecorder] flushed {} rows → {}",
                    day_rows.len(), path.display(),
                ),
                Err(e) => warn!(
                    "[LatencyRecorder] write {} failed: {} — {} rows lost",
                    path.display(), e, day_rows.len(),
                ),
            }
        }
    }
}

/// Current wall-clock 5-minute bucket index.
fn current_bucket() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() / FLUSH_BUCKET_SECS)
        .unwrap_or(0)
}

/// The UTC calendar date (`YYYYMMDD`) of an epoch-ms timestamp — the
/// daily-rotation file key. `from_timestamp_millis` yields a UTC
/// `DateTime`, so the format is UTC regardless of the host timezone.
/// Empty on the (impossible) out-of-range case.
fn utc_date(epoch_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(epoch_ms as i64)
        .map(|dt| dt.format("%Y%m%d").to_string())
        .unwrap_or_default()
}

/// Format an epoch-ms timestamp as a local-time ISO-8601 string with
/// millisecond precision. Empty on the (impossible) out-of-range case.
fn format_local(epoch_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(epoch_ms as i64)
        .map(|dt| {
            dt.with_timezone(&chrono::Local)
                .format("%Y-%m-%dT%H:%M:%S%.3f")
                .to_string()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("hexbot_latrec_{}_{}_{}", tag, std::process::id(), nanos))
    }

    /// Sorted list of daily `<YYYYMMDD>.csv` files in `dir` (the daily
    /// rotation produces date-named files, so tests glob rather than
    /// assume a fixed name).
    fn list_probe_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut v: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| {
                        p.file_name()
                            .and_then(|n| n.to_str())
                            .and_then(|n| n.strip_suffix(".csv"))
                            .map(|s| s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()))
                            .unwrap_or(false)
                    })
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    #[test]
    fn writes_header_and_rows_on_flush() {
        let dir = tmp_dir("hdr");
        let rec = LatencyRecorder::new(dir.to_str().unwrap(), "20260614_000000");
        rec.record("maker01", "place", 42.5, "ok".to_string());
        rec.record("maker01", "cancel", 7.25, "http_404".to_string());
        rec.flush();

        // Records are stamped "now", so they land in today's UTC file —
        // exactly one daily file (the test runs well within one UTC day).
        let files = list_probe_files(&dir);
        assert_eq!(files.len(), 1, "expected one daily file, got {:?}", files);
        let path = &files[0];
        let name = path.file_name().unwrap().to_str().unwrap();
        let stem = name.strip_suffix(".csv").unwrap_or("");
        assert!(
            stem.len() == 8 && stem.bytes().all(|b| b.is_ascii_digit()),
            "daily filename shape <YYYYMMDD>.csv: {}", name,
        );

        let body = std::fs::read_to_string(path).expect("file written");
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines[0], "epoch_ms,iso_local,instance_id,kind,rtt_ms,status");
        assert_eq!(lines.len(), 3, "header + 2 rows, got: {:?}", lines);
        assert!(lines[1].contains(",maker01,place,42.500,ok"), "row1={}", lines[1]);
        assert!(lines[2].contains(",maker01,cancel,7.250,http_404"), "row2={}", lines[2]);

        // Second flush with no new rows must NOT re-emit the header.
        rec.flush();
        let body2 = std::fs::read_to_string(path).unwrap();
        assert_eq!(body2.lines().count(), 3, "empty flush must not append");

        // Appending more rows continues without a second header.
        rec.record("maker01", "place", 1.0, "ok".to_string());
        rec.flush();
        let body3 = std::fs::read_to_string(path).unwrap();
        let lines3: Vec<&str> = body3.lines().collect();
        assert_eq!(lines3.len(), 4);
        assert_eq!(lines3[0], "epoch_ms,iso_local,instance_id,kind,rtt_ms,status");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn maybe_flush_is_noop_within_same_bucket() {
        let dir = tmp_dir("bucket");
        let rec = LatencyRecorder::new(dir.to_str().unwrap(), "20260614_000001");
        rec.record("m", "place", 1.0, "ok".to_string());
        // Same 5-min bucket as construction → no flush, no file yet.
        rec.maybe_flush();
        assert!(list_probe_files(&dir).is_empty(), "maybe_flush flushed within the same bucket");
        // Forcing the bucket back makes maybe_flush fire.
        rec.last_flush_bucket.store(0, Ordering::Relaxed);
        rec.maybe_flush();
        assert_eq!(list_probe_files(&dir).len(), 1, "maybe_flush should flush after crossing a bucket");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotates_files_at_utc_midnight() {
        let dir = tmp_dir("rotate");
        let rec = LatencyRecorder::new(dir.to_str().unwrap(), "20260614_120000");
        // Two rows straddling 2026-06-14T00:00:00Z: one 1ms before the
        // UTC boundary, one exactly at it. A single flush must route them
        // into two distinct daily files.
        let midnight_ms = chrono::DateTime::parse_from_rfc3339("2026-06-14T00:00:00Z")
            .unwrap()
            .timestamp_millis() as u64;
        if let Ok(mut b) = rec.buf.lock() {
            b.push(Row {
                epoch_ms: midnight_ms - 1,
                instance_id: "m".to_string(),
                kind: "place",
                rtt_ms: 1.0,
                status: "ok".to_string(),
            });
            b.push(Row {
                epoch_ms: midnight_ms,
                instance_id: "m".to_string(),
                kind: "cancel",
                rtt_ms: 2.0,
                status: "ok".to_string(),
            });
        }
        rec.flush();

        let names: Vec<String> = list_probe_files(&dir)
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(
            names,
            vec![
                "20260613.csv".to_string(),
                "20260614.csv".to_string(),
            ],
            "rows straddling 00:00 UTC must split into two daily files; got {:?}", names,
        );
        // Each day's file has its own header + exactly one row.
        for n in &names {
            let body = std::fs::read_to_string(dir.join(n)).unwrap();
            assert_eq!(body.lines().count(), 2, "{n}: header + 1 row");
            assert_eq!(body.lines().next().unwrap(), "epoch_ms,iso_local,instance_id,kind,rtt_ms,status");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
