//! Process-wide bounded executor for cold, blocking, one-shot maintenance work.
//!
//! Long-lived connection/feed actors keep their dedicated owners. Short REST,
//! audit and cleanup jobs use this fixed pool so they cannot create arbitrary
//! OS threads or inherit a strategy core's affinity at runtime.

use std::sync::OnceLock;

const JOB_CAPACITY: usize = 1_024;
const WORKER_COUNT: usize = 4;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Executor {
    tx: crossbeam_channel::Sender<Job>,
}

static EXECUTOR: OnceLock<Result<Executor, String>> = OnceLock::new();

impl Executor {
    fn start() -> Result<Self, String> {
        let (tx, rx) = crossbeam_channel::bounded::<Job>(JOB_CAPACITY);
        let mut handles = Vec::with_capacity(WORKER_COUNT);
        for worker_id in 0..WORKER_COUNT {
            let worker_rx = rx.clone();
            let name = format!("hex-bg-job-{worker_id}");
            match std::thread::Builder::new()
                .name(name.clone())
                .spawn(move || {
                    crate::os_tune::pin_background(&name);
                    while let Ok(job) = worker_rx.recv() {
                        job();
                    }
                }) {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    drop(tx);
                    drop(handles);
                    return Err(format!("spawn bounded background worker: {error}"));
                }
            }
        }
        // The process-wide sender owns worker lifetime; JoinHandles are
        // intentionally detached because runtime teardown exits the process.
        drop(handles);
        Ok(Self { tx })
    }
}

fn executor() -> Result<&'static Executor, String> {
    EXECUTOR
        .get_or_init(Executor::start)
        .as_ref()
        .map_err(Clone::clone)
}

pub fn prewarm() -> Result<(), String> {
    executor().map(|_| ())
}

pub fn try_submit(job: impl FnOnce() + Send + 'static) -> Result<(), String> {
    executor()?
        .tx
        .try_send(Box::new(job))
        .map_err(|error| match error {
            crossbeam_channel::TrySendError::Full(_) => {
                "bounded runtime background job queue is full".to_string()
            }
            crossbeam_channel::TrySendError::Disconnected(_) => {
                "bounded runtime background job executor is disconnected".to_string()
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executes_submitted_job() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        try_submit(move || {
            let _ = tx.send(11_u8);
        })
        .unwrap();
        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(1)), Ok(11));
    }
}
