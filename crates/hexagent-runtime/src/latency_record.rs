//! Typed, asynchronous place/cancel latency recording.
//!
//! The response path only stamps a fixed-size row and `try_send`s it to a
//! bounded telemetry queue.  A single background writer owns buffering,
//! date rotation, CSV formatting, and filesystem I/O.  It therefore has no
//! global mutex, heap allocation, string formatting, or blocking operation on
//! the HTTP completion path.

use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, Receiver, Sender};
use log::{info, warn};

const FLUSH_BUCKET_SECS: u64 = 300;
const RECORD_QUEUE_CAPACITY: usize = 16_384;
const INSTANCE_ID_BYTES: usize = 96;

static RECORDER: OnceLock<LatencyRecorder> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    Place,
    Cancel,
    ProbePlace,
    ProbeCancel,
}

impl RequestKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Place => "place",
            Self::Cancel => "cancel",
            Self::ProbePlace => "probe_place",
            Self::ProbeCancel => "probe_cancel",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestStatus {
    Ok,
    Timeout,
    Http(u16),
    TransportError,
    InvalidResponse,
    Error,
}

impl RequestStatus {
    fn write_csv(self, out: &mut String) {
        match self {
            Self::Ok => out.push_str("ok"),
            Self::Timeout => out.push_str("timeout"),
            Self::Http(code) => {
                let _ = write!(out, "http_{code}");
            }
            Self::TransportError => out.push_str("transport_error"),
            Self::InvalidResponse => out.push_str("invalid_response"),
            Self::Error => out.push_str("error"),
        }
    }
}

#[derive(Clone, Copy)]
struct InlineInstanceId {
    len: u8,
    bytes: [u8; INSTANCE_ID_BYTES],
}

impl InlineInstanceId {
    #[inline]
    fn new(value: &str) -> Self {
        let mut bytes = [0u8; INSTANCE_ID_BYTES];
        let len = value.len().min(INSTANCE_ID_BYTES);
        bytes[..len].copy_from_slice(&value.as_bytes()[..len]);
        Self {
            len: len as u8,
            bytes,
        }
    }

    fn as_str(&self) -> &str {
        // The prefix of a valid UTF-8 string can end within a codepoint when
        // truncated. Instance IDs are protocol ASCII, so keep the fallback
        // defensive without adding work to the recording path.
        std::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or("invalid-instance-id")
    }
}

#[derive(Clone, Copy)]
struct Row {
    epoch_ms: u64,
    instance_id: InlineInstanceId,
    kind: RequestKind,
    rtt_ms: f64,
    status: RequestStatus,
}

enum WriterMessage {
    Row(Row),
    Flush(Option<Sender<()>>),
}

struct LatencyRecorder {
    tx: Sender<WriterMessage>,
    last_flush_bucket: AtomicU64,
    dropped: AtomicU64,
}

pub fn init(dir: &str, start_label: &str) -> bool {
    RECORDER.set(LatencyRecorder::new(dir, start_label)).is_ok()
}

#[inline]
pub fn is_active() -> bool {
    RECORDER.get().is_some()
}

/// Enqueue one fully typed record. This is the hot-path API.
#[inline]
pub fn record(
    instance_id: &str,
    kind: RequestKind,
    rtt_ms: f64,
    status: RequestStatus,
) {
    if let Some(recorder) = RECORDER.get() {
        recorder.record(instance_id, kind, rtt_ms, status);
    }
}

/// Request a boundary flush. The writer also checks the wall-clock bucket on
/// its own, so a saturated telemetry queue never makes rotation dependent on
/// a trading thread.
pub fn maybe_flush() {
    if let Some(recorder) = RECORDER.get() {
        recorder.maybe_flush();
    }
}

/// Synchronously drain and persist queued records. Used only during shutdown.
pub fn flush() {
    if let Some(recorder) = RECORDER.get() {
        recorder.flush();
    }
}

impl LatencyRecorder {
    fn new(dir: &str, start_label: &str) -> Self {
        let dir = PathBuf::from(dir);
        if let Err(error) = fs::create_dir_all(&dir) {
            warn!(
                "[LatencyRecorder] create_dir_all({}) failed: {} — latency records may not persist",
                dir.display(),
                error,
            );
        }
        info!(
            "[LatencyRecorder] typed async records → {}/<UTC-date>.csv (run started {}, flush every {}s)",
            dir.display(),
            start_label,
            FLUSH_BUCKET_SECS,
        );
        let (tx, rx) = bounded(RECORD_QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("latency-record-writer".into())
            .spawn(move || writer_loop(dir, rx))
            .expect("spawn latency record writer");
        Self {
            tx,
            last_flush_bucket: AtomicU64::new(current_bucket()),
            dropped: AtomicU64::new(0),
        }
    }

    #[inline]
    fn record(
        &self,
        instance_id: &str,
        kind: RequestKind,
        rtt_ms: f64,
        status: RequestStatus,
    ) {
        let row = Row {
            epoch_ms: epoch_ms(),
            instance_id: InlineInstanceId::new(instance_id),
            kind,
            rtt_ms,
            status,
        };
        if self.tx.try_send(WriterMessage::Row(row)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn maybe_flush(&self) {
        let bucket = current_bucket();
        let last = self.last_flush_bucket.load(Ordering::Relaxed);
        if bucket > last
            && self
                .last_flush_bucket
                .compare_exchange(last, bucket, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let _ = self.tx.try_send(WriterMessage::Flush(None));
        }
    }

    fn flush(&self) {
        let (done_tx, done_rx) = bounded(1);
        if self
            .tx
            .send(WriterMessage::Flush(Some(done_tx)))
            .is_ok()
        {
            let _ = done_rx.recv();
        }
        let dropped = self.dropped.swap(0, Ordering::Relaxed);
        if dropped != 0 {
            warn!("[LatencyRecorder] dropped {dropped} records because telemetry queue was full");
        }
    }

    #[cfg(test)]
    fn record_at(&self, row: Row) {
        self.tx.send(WriterMessage::Row(row)).unwrap();
    }
}

fn writer_loop(dir: PathBuf, rx: Receiver<WriterMessage>) {
    let mut rows = Vec::with_capacity(4096);
    let mut bucket = current_bucket();
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(WriterMessage::Row(row)) => rows.push(row),
            Ok(WriterMessage::Flush(done)) => {
                flush_rows(&dir, &mut rows);
                if let Some(done) = done {
                    let _ = done.send(());
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                flush_rows(&dir, &mut rows);
                return;
            }
        }
        let now_bucket = current_bucket();
        if now_bucket > bucket {
            bucket = now_bucket;
            flush_rows(&dir, &mut rows);
        }
    }
}

fn flush_rows(dir: &Path, rows: &mut Vec<Row>) {
    if rows.is_empty() {
        return;
    }
    let mut drained = Vec::with_capacity(rows.capacity());
    std::mem::swap(rows, &mut drained);
    let mut by_date: std::collections::BTreeMap<String, Vec<&Row>> =
        std::collections::BTreeMap::new();
    for row in &drained {
        by_date.entry(utc_date(row.epoch_ms)).or_default().push(row);
    }
    for (date, day_rows) in by_date {
        let path = dir.join(format!("{date}.csv"));
        let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => file,
            Err(error) => {
                warn!(
                    "[LatencyRecorder] open {} failed: {} — dropping {} rows",
                    path.display(),
                    error,
                    day_rows.len(),
                );
                continue;
            }
        };
        let need_header = file.metadata().map(|meta| meta.len() == 0).unwrap_or(false);
        let mut out = String::with_capacity(day_rows.len() * 96 + 64);
        if need_header {
            out.push_str("epoch_ms,iso_local,instance_id,kind,rtt_ms,status\n");
        }
        for row in &day_rows {
            let _ = write!(
                out,
                "{},{},{},{},{:.3},",
                row.epoch_ms,
                format_local(row.epoch_ms),
                row.instance_id.as_str(),
                row.kind.as_str(),
                row.rtt_ms,
            );
            row.status.write_csv(&mut out);
            out.push('\n');
        }
        match file.write_all(out.as_bytes()) {
            Ok(()) => info!(
                "[LatencyRecorder] flushed {} rows → {}",
                day_rows.len(),
                path.display(),
            ),
            Err(error) => warn!(
                "[LatencyRecorder] write {} failed: {} — {} rows lost",
                path.display(),
                error,
                day_rows.len(),
            ),
        }
    }
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn current_bucket() -> u64 {
    epoch_ms() / 1_000 / FLUSH_BUCKET_SECS
}

fn utc_date(epoch_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(epoch_ms as i64)
        .map(|date_time| date_time.format("%Y%m%d").to_string())
        .unwrap_or_default()
}

fn format_local(epoch_ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(epoch_ms as i64)
        .map(|date_time| {
            date_time
                .with_timezone(&chrono::Local)
                .format("%Y-%m-%dT%H:%M:%S%.3f")
                .to_string()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "hexbot_latrec_{}_{}_{}",
            tag,
            std::process::id(),
            epoch_ms()
        ))
    }

    fn list_probe_files(dir: &Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                    .filter(|path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .and_then(|name| name.strip_suffix(".csv"))
                            .map(|stem| {
                                stem.len() == 8
                                    && stem.bytes().all(|byte| byte.is_ascii_digit())
                            })
                            .unwrap_or(false)
                    })
                    .collect()
            })
            .unwrap_or_default();
        files.sort();
        files
    }

    #[test]
    fn writes_header_and_typed_rows_on_flush() {
        let dir = tmp_dir("typed");
        let recorder = LatencyRecorder::new(dir.to_str().unwrap(), "20260614_000000");
        recorder.record("maker01", RequestKind::Place, 42.5, RequestStatus::Ok);
        recorder.record(
            "maker01",
            RequestKind::Cancel,
            7.25,
            RequestStatus::Http(404),
        );
        recorder.flush();
        let files = list_probe_files(&dir);
        assert_eq!(files.len(), 1);
        let body = std::fs::read_to_string(&files[0]).unwrap();
        let lines: Vec<_> = body.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[1].contains(",maker01,place,42.500,ok"));
        assert!(lines[2].contains(",maker01,cancel,7.250,http_404"));
        recorder.flush();
        assert_eq!(std::fs::read_to_string(&files[0]).unwrap().lines().count(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn maybe_flush_is_noop_within_same_bucket() {
        let dir = tmp_dir("bucket");
        let recorder = LatencyRecorder::new(dir.to_str().unwrap(), "start");
        recorder.record("m", RequestKind::Place, 1.0, RequestStatus::Ok);
        recorder.maybe_flush();
        assert!(list_probe_files(&dir).is_empty());
        recorder.last_flush_bucket.store(0, Ordering::Relaxed);
        recorder.maybe_flush();
        // Flush messages preserve FIFO order behind the record.
        for _ in 0..100 {
            if !list_probe_files(&dir).is_empty() {
                break;
            }
            std::thread::yield_now();
        }
        assert_eq!(list_probe_files(&dir).len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotates_files_at_utc_midnight() {
        let dir = tmp_dir("rotate");
        let recorder = LatencyRecorder::new(dir.to_str().unwrap(), "start");
        let midnight_ms = chrono::DateTime::parse_from_rfc3339("2026-06-14T00:00:00Z")
            .unwrap()
            .timestamp_millis() as u64;
        for (epoch_ms, kind) in [
            (midnight_ms - 1, RequestKind::Place),
            (midnight_ms, RequestKind::Cancel),
        ] {
            recorder.record_at(Row {
                epoch_ms,
                instance_id: InlineInstanceId::new("m"),
                kind,
                rtt_ms: 1.0,
                status: RequestStatus::Ok,
            });
        }
        recorder.flush();
        let names: Vec<_> = list_probe_files(&dir)
            .iter()
            .map(|path| path.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["20260613.csv", "20260614.csv"]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
