//! Account-owned record batches for the shared hourly audit artifact.
//!
//! All callers are existing SCHED_OTHER audit owners, after the bounded audit
//! lane. Serialize only into an owner-local buffer, then append complete rows
//! under an advisory file lock. A BufWriter is unsuitable: it can flush half
//! a JSON value while another account appends the next half of another value.
//! The cold file lock also coordinates cooperating processes without a global
//! mutable registry or a second lifecycle queue. No quote/order owner uses it.

use std::fs::File;
use std::io::{self, Write};

const BATCH_BYTES: usize = 64 * 1024;

pub(super) struct RecordBatchWriter {
    file: File,
    pending: Vec<u8>,
    poisoned: bool,
}

impl RecordBatchWriter {
    pub(super) fn new(file: File) -> Self {
        Self {
            file,
            pending: Vec::with_capacity(BATCH_BYTES),
            poisoned: false,
        }
    }

    pub(super) fn write_record(&mut self, record: &serde_json::Value) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "audit append failed; writer requires recovery",
            ));
        }
        let start = self.pending.len();
        if let Err(error) = serde_json::to_writer(&mut self.pending, record) {
            self.pending.truncate(start);
            return Err(error.into());
        }
        self.pending.push(b'\n');
        if self.pending.len() >= BATCH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    pub(super) fn flush(&mut self) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "audit append failed; writer requires recovery",
            ));
        }
        if self.pending.is_empty() {
            return Ok(());
        }
        let _lock = match FileAppendLock::acquire(&self.file) {
            Ok(lock) => lock,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        if let Err(error) = self.file.write_all(&self.pending) {
            // write_all can have appended a prefix. Replaying the entire batch
            // would duplicate rows; preserve the failure for explicit recovery.
            self.poisoned = true;
            return Err(error);
        }
        self.pending.clear();
        Ok(())
    }
}

impl Drop for RecordBatchWriter {
    fn drop(&mut self) {
        if !self.pending.is_empty() && !self.poisoned {
            if let Err(error) = self.flush() {
                log::error!("[order_audit] final batch flush failed: {error}");
            }
        }
    }
}

#[cfg(unix)]
struct FileAppendLock(std::os::fd::RawFd);

#[cfg(unix)]
impl FileAppendLock {
    fn acquire(file: &File) -> io::Result<Self> {
        use std::os::fd::AsRawFd;
        let fd = file.as_raw_fd();
        loop {
            if unsafe { libc::flock(fd, libc::LOCK_EX) } == 0 {
                return Ok(Self(fd));
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}

#[cfg(unix)]
impl Drop for FileAppendLock {
    fn drop(&mut self) {
        loop {
            if unsafe { libc::flock(self.0, libc::LOCK_UN) } == 0 {
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                log::error!("[order_audit] append lock release failed: {error}");
                break;
            }
        }
    }
}

#[cfg(not(unix))]
struct FileAppendLock;
#[cfg(not(unix))]
impl FileAppendLock {
    fn acquire(_: &File) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "shared audit append requires an advisory file lock",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_account_batches_keep_large_rows_complete_ordered_and_unique() {
        let dir = std::env::temp_dir().join(format!(
            "audit-batches-{}-{}",
            std::process::id(),
            crate::types::now_ns()
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("shared.jsonl");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
        let workers: Vec<_> = (0..4).map(|account| {
            let path = path.clone(); let barrier = barrier.clone();
            std::thread::spawn(move || {
                let file = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
                let mut writer = RecordBatchWriter::new(file);
                barrier.wait();
                for sequence in 0..150 {
                    let payload = "x".repeat(if sequence % 7 == 0 { BATCH_BYTES + 199 } else { 1803 });
                    writer.write_record(&serde_json::json!({"account": account, "sequence": sequence, "payload": payload})).unwrap();
                    if sequence % 11 == 0 { writer.flush().unwrap(); }
                }
                // Drop must append the remaining complete batch under the lock.
            })
        }).collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.ends_with('\n'));
        let mut next = [0; 4];
        for line in content.lines() {
            let row: serde_json::Value = serde_json::from_str(line).unwrap();
            let account = row["account"].as_u64().unwrap() as usize;
            assert_eq!(row["sequence"].as_u64().unwrap(), next[account]);
            next[account] += 1;
        }
        assert_eq!(next, [150; 4]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_write_is_reported_and_cannot_replay_a_partial_batch() {
        let path = std::env::temp_dir().join(format!(
            "audit-failure-{}-{}",
            std::process::id(),
            crate::types::now_ns()
        ));
        std::fs::write(&path, []).unwrap();
        let mut writer = RecordBatchWriter::new(File::open(&path).unwrap());
        writer
            .write_record(&serde_json::json!({"account": 1}))
            .unwrap();
        assert!(writer.flush().is_err());
        assert!(writer.poisoned);
        assert!(writer
            .write_record(&serde_json::json!({"account": 2}))
            .is_err());
        assert!(writer.flush().is_err());
        drop(writer);
        assert!(std::fs::read(&path).unwrap().is_empty());
        std::fs::remove_file(path).unwrap();
    }
}
