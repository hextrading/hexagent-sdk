//! Calibration line source — transparently feed the latency calibration
//! parsers from EITHER a raw `live*.log` OR a pre-extracted **parquet
//! archive**.
//!
//! ## Why an archive
//!
//! When `sim_latency_calibrate_from` points at log files, the engine
//! re-parses them on *every* backtest through three independent passes
//! (`latency::calibrate_from_logs`, `per_event_rtt::extract_per_event_rtt`,
//! `per_event_rtt::extract_taker_overhead`). Real sessions are large
//! (live101.log ≈ 99 MB, live102.log ≈ 119 MB) and only ~40 % of lines
//! are relevant to latency calibration — the rest (spot feed, quotes,
//! myindex, orderbook) is scanned and discarded every run.
//!
//! `scripts/extract_latency_calib.py` does that filtering **once**: it
//! keeps exactly the lines any of the three parsers can consume — the
//! union of their substring predicates — and stores them, **verbatim and
//! in original order**, as a zstd-compressed parquet file with two
//! columns:
//!
//!   * `kind` — a cosmetic classification tag (for human inspection /
//!     `SELECT … WHERE kind=…`); **not read by this module**.
//!   * `line` — the original log line, byte-for-byte (sans trailing
//!     newline, exactly as `BufRead::lines()` would yield it).
//!
//! ## Why the result is identical to parsing the raw log
//!
//! The three parsers process input strictly line-by-line; a line that
//! matches none of their predicates has **zero** effect on any
//! accumulator (no map insert, no counter bump, no `continue`-with-side-
//! effect). The extractor drops only such no-effect lines, and preserves
//! the relative order of every kept line. Therefore feeding the kept
//! lines back through the *unchanged* per-line parser bodies reproduces
//! every statistic — percentiles, AR(1) ρ, cross-correlation, GPD tail,
//! per-event quantiles, timeout rates, taker overhead — bit-for-bit.
//! Each parser keeps applying its **own** predicate to the verbatim line
//! text, so the subtle cross-parser differences (e.g. `calibrate_from_logs`
//! counting only `[PolymarketTrade] … → NewOrderTimeout` timeout lines
//! while `extract_per_event_rtt` counts every line containing
//! `NewOrderTimeout`) are preserved automatically — this module never
//! re-classifies, it only changes where the line text comes from.
//!
//! Detection is by file extension: `.parquet` / `.pq` → archive, anything
//! else → raw log. The engine's existing `is_dir()` check still routes a
//! record-replay *directory* to the replay path before this is reached.

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

/// Name of the parquet column holding the verbatim log line.
const LINE_COLUMN: &str = "line";

/// Open a calibration source and return an iterator of its log lines.
///
/// * `*.parquet` / `*.pq` → decode the `line` column of the archive,
///   yielding one `String` per row **in stored (= original log) order**.
/// * anything else → the file's lines verbatim (`BufReader::lines()`),
///   identical to the legacy path.
///
/// Errors that abort the whole source (open failure, malformed parquet
/// footer, missing `line` column type) surface as the `Err` of this
/// function or as a single `Err` item from the iterator; per-line decode
/// never silently drops rows.
pub fn calib_lines(
    path: &str,
) -> io::Result<Box<dyn Iterator<Item = io::Result<String>>>> {
    if is_parquet_path(path) {
        Ok(Box::new(ParquetLineIter::open(path)?))
    } else {
        let file = File::open(path)?;
        Ok(Box::new(BufReader::new(file).lines()))
    }
}

fn is_parquet_path(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("parquet") || e.eq_ignore_ascii_case("pq"))
        .unwrap_or(false)
}

/// Streaming iterator over the `line` column of a parquet archive. Pulls
/// one `RecordBatch` at a time (so memory stays bounded by a single batch
/// rather than the whole archive) and yields its rows in order.
struct ParquetLineIter {
    reader: ParquetRecordBatchReader,
    batch: Option<RecordBatch>,
    row: usize,
}

impl ParquetLineIter {
    fn open(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| to_io(format!("open parquet archive {path}: {e}")))?;
        let reader = builder
            .build()
            .map_err(|e| to_io(format!("build parquet reader {path}: {e}")))?;
        Ok(Self { reader, batch: None, row: 0 })
    }
}

impl Iterator for ParquetLineIter {
    type Item = io::Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Drain the current batch first.
            if let Some(batch) = &self.batch {
                if self.row < batch.num_rows() {
                    let col = match batch.column_by_name(LINE_COLUMN) {
                        Some(c) => c,
                        None => {
                            return Some(Err(to_io(format!(
                                "parquet archive missing `{LINE_COLUMN}` column"
                            ))))
                        }
                    };
                    let arr = match col.as_any().downcast_ref::<StringArray>() {
                        Some(a) => a,
                        None => {
                            return Some(Err(to_io(format!(
                                "parquet `{LINE_COLUMN}` column is not Utf8/StringArray"
                            ))))
                        }
                    };
                    let i = self.row;
                    self.row += 1;
                    // A null line is impossible from the extractor, but be
                    // defensive: treat it as an empty line (no-op for every
                    // parser predicate) rather than panicking.
                    let s = if arr.is_null(i) {
                        String::new()
                    } else {
                        arr.value(i).to_string()
                    };
                    return Some(Ok(s));
                }
                // Batch exhausted — fall through to fetch the next one.
                self.batch = None;
                self.row = 0;
            }

            match self.reader.next() {
                None => return None,
                Some(Ok(b)) => {
                    self.batch = Some(b);
                    self.row = 0;
                }
                Some(Err(e)) => return Some(Err(to_io(format!("parquet batch read: {e}")))),
            }
        }
    }
}

fn to_io(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringBuilder;
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// `.log` source behaves exactly like `BufReader::lines()` — verbatim,
    /// in order, trailing newline stripped.
    #[test]
    fn log_source_yields_verbatim_lines() {
        let mut f = tempfile::Builder::new().suffix(".log").tempfile().unwrap();
        // Lines deliberately contain commas, quotes and the `→` arrow to
        // prove the log path is untouched by any CSV/encoding logic.
        write!(
            f,
            "alpha, with comma\n\"quoted\" line\nx → NewOrderTimeout\n"
        )
        .unwrap();
        f.flush().unwrap();
        let got: Vec<String> = calib_lines(f.path().to_str().unwrap())
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            got,
            vec![
                "alpha, with comma".to_string(),
                "\"quoted\" line".to_string(),
                "x → NewOrderTimeout".to_string(),
            ]
        );
    }

    /// Parquet archive round-trips the `line` column verbatim and in row
    /// order across multiple batches — the property `calibrate_from_logs`
    /// & friends rely on for identical results.
    #[test]
    fn parquet_source_round_trips_lines_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("arch.parquet");

        // Lines with commas / quotes / arrows — must come back byte-exact.
        let lines = vec![
            "2026-05-14T17:53:26.557Z  INFO [latency] polymarket.http.place_order n=19 p50=620.23ms".to_string(),
            "2026-05-14T21:50:02.565Z  INFO [PolymarketTrade] Submit BUY @ 0.330, qty=5 coid=1778781146557".to_string(),
            "x → NewOrderTimeout \"weird\", value".to_string(),
        ];

        let schema = Arc::new(Schema::new(vec![
            Field::new("kind", DataType::Utf8, false),
            Field::new(LINE_COLUMN, DataType::Utf8, false),
        ]));
        let mut kind_b = StringBuilder::new();
        let mut line_b = StringBuilder::new();
        for l in &lines {
            kind_b.append_value("x");
            line_b.append_value(l);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(kind_b.finish()), Arc::new(line_b.finish())],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            // Tiny batch size to force >1 row group → exercises the
            // multi-batch path of the iterator.
            .set_max_row_group_size(2)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let got: Vec<String> = calib_lines(path.to_str().unwrap())
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(got, lines);
    }
}
