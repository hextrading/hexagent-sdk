//! Bounded owners for blocking control-plane RPCs.
//!
//! This pool is deliberately separate from `background_jobs`: a background
//! maintenance job may synchronously await a pair of RPC results, so running
//! those RPC closures on the same workers can deadlock when every background
//! worker is waiting. Only cold paths may submit here.

use std::sync::OnceLock;

const JOB_CAPACITY: usize = 256;
const WORKER_COUNT: usize = 8;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct Executor {
    tx: crossbeam_channel::Sender<Job>,
}

static EXECUTOR: OnceLock<Result<Executor, String>> = OnceLock::new();

impl Executor {
    fn start() -> Result<Self, String> {
        let (tx, rx) = crossbeam_channel::bounded::<Job>(JOB_CAPACITY);
        for worker_id in 0..WORKER_COUNT {
            let worker_rx = rx.clone();
            let name = format!("hex-cold-rpc-{worker_id}");
            std::thread::Builder::new()
                .name(name.clone())
                .spawn(move || {
                    crate::os_tune::pin_background(&name);
                    while let Ok(job) = worker_rx.recv() {
                        job();
                    }
                })
                .map_err(|error| format!("spawn cold RPC owner: {error}"))?;
        }
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
                "bounded cold RPC owner queue is full".to_string()
            }
            crossbeam_channel::TrySendError::Disconnected(_) => {
                "bounded cold RPC owner queue is disconnected".to_string()
            }
        })
}

#[cfg(test)]
mod tests {
    #[test]
    fn executes_submitted_rpc() {
        let (tx, rx) = crossbeam_channel::bounded(1);
        super::try_submit(move || {
            let _ = tx.send(17_u8);
        })
        .unwrap();
        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(1)), Ok(17));
    }
}
