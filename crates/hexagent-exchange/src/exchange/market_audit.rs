//! Opt-in RECORD diagnostics. Producers publish fixed-size records to a bounded
//! FIFO; the existing pinned recorder is the sole formatter/file writer. No new
//! threads, global state, secrets, blocking send or market-state authority.
use super::MarketEvent;
use serde::Serialize;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

#[derive(Clone, Copy, Debug, Serialize)]
pub struct AuditRecord {
    pub stage: &'static str,
    pub action: &'static str,
    pub source_ns: u64,
    pub received_ns: u64,
    pub price: f64,
    pub symbol_id: u64,
    pub channel: usize,
    pub connection: u64,
    pub seq: u64,
    pub depth: usize,
}
impl AuditRecord {
    pub fn empty(stage: &'static str, action: &'static str) -> Self {
        Self {
            stage,
            action,
            source_ns: 0,
            received_ns: crate::types::now_ns(),
            price: 0.,
            symbol_id: 0,
            channel: 0,
            connection: 0,
            seq: 0,
            depth: 0,
        }
    }
}
/// Stable symbol fingerprint, computed only when diagnostics are enabled.
pub fn symbol_id(s: &str) -> u64 {
    s.bytes().fold(0xcbf29ce484222325, |h, b| {
        (h ^ b as u64).wrapping_mul(0x100000001b3)
    })
}
#[derive(Default)]
struct Counters {
    dropped: AtomicU64,
    high_water: AtomicU64,
}
#[derive(Clone)]
pub struct AuditSink {
    tx: crossbeam_channel::Sender<AuditRecord>,
    counters: Arc<Counters>,
}
pub struct AuditWriter {
    rx: crossbeam_channel::Receiver<AuditRecord>,
    counters: Arc<Counters>,
    file: BufWriter<File>,
    written: u64,
}
impl AuditSink {
    pub fn record(&self, record: AuditRecord) {
        if self.tx.try_send(record).is_err() {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
        self.counters
            .high_water
            .fetch_max(self.tx.len() as u64, Ordering::Relaxed);
    }
    pub fn event(
        &self,
        stage: &'static str,
        action: &'static str,
        event: &MarketEvent,
        depth: usize,
    ) {
        let mut r = AuditRecord::empty(stage, action);
        r.depth = depth;
        if let MarketEvent::SpotPrice(p) = event {
            r.source_ns = p.timestamp_ns;
            r.received_ns = p.local_timestamp_ns;
            r.price = p.price;
            r.symbol_id = symbol_id(&p.symbol);
            r.channel = usize::from(p.symbol.ends_with("/twap/60s"));
        } else if action == "accepted" || action == "received" {
            return;
        }
        self.record(r);
    }
}
impl AuditWriter {
    pub fn create(path: &Path, capacity: usize) -> std::io::Result<(AuditSink, Self)> {
        let file = BufWriter::new(File::options().write(true).create_new(true).open(path)?);
        let (tx, rx) = crossbeam_channel::bounded(capacity);
        let counters = Arc::new(Counters::default());
        Ok((
            AuditSink {
                tx,
                counters: counters.clone(),
            },
            Self {
                rx,
                counters,
                file,
                written: 0,
            },
        ))
    }
    pub fn drain(&mut self, budget: usize) -> std::io::Result<()> {
        for _ in 0..budget {
            let Ok(r) = self.rx.try_recv() else {
                break;
            };
            serde_json::to_writer(&mut self.file, &r)?;
            self.file.write_all(b"\n")?;
            self.written += 1;
        }
        Ok(())
    }
    pub fn finish(&mut self) -> std::io::Result<()> {
        self.drain(65_536)?;
        serde_json::to_writer(
            &mut self.file,
            &serde_json::json!({"stage":"audit_summary", "written":self.written,
            "dropped":self.counters.dropped.load(Ordering::Relaxed), "high_water":self.counters.high_water.load(Ordering::Relaxed),
            "remaining":self.rx.len(), "capacity":self.rx.capacity(),
            "scope":"through recorder exit; shutdown tail excluded from completeness windows"}),
        )?;
        self.file.write_all(b"\n")?;
        self.file.flush()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_fifo_reports_overflow_and_isolates_runs() {
        let d = tempfile::tempdir().unwrap();
        let (sink, mut writer) = AuditWriter::create(&d.path().join("a"), 2).unwrap();
        let (other, mut other_writer) = AuditWriter::create(&d.path().join("b"), 2).unwrap();
        for seq in 1..=3 {
            let mut r = AuditRecord::empty("test", "test");
            r.seq = seq;
            sink.record(r);
        }
        other.record(AuditRecord::empty("other", "other"));
        writer.finish().unwrap();
        other_writer.finish().unwrap();
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(d.path().join("a"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows[0]["seq"], 1);
        assert_eq!(rows[1]["seq"], 2);
        assert_eq!(rows[2]["dropped"], 1);
        assert_eq!(other.counters.dropped.load(Ordering::Relaxed), 0);
    }
}
