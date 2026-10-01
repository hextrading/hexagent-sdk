//! The websocket decoder may return buffered frames without polling a Tokio
//! socket, so socket cooperative budgets do not bound a reader's ready loop.
//! An owner-local turn budget bounds BOTH active/standby selection and candidate
//! seeding. It never sleeps, changes event order or creates another queue.
pub(super) struct ClobCooperativeBudget {
    remaining: u8,
}

const MAX_TURNS: u8 = 8;

impl ClobCooperativeBudget {
    pub(super) fn new() -> Self {
        Self {
            remaining: MAX_TURNS,
        }
    }

    #[inline]
    pub(super) async fn checkpoint(&mut self) {
        if self.remaining == 0 {
            self.remaining = MAX_TURNS;
            tokio::task::yield_now().await;
        }
        self.remaining -= 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn buffered_ready_loop_yields_to_sibling_and_preserves_order() {
        let processed = Arc::new(AtomicUsize::new(0));
        let peer = processed.clone();
        let sibling = tokio::spawn(async move { peer.load(Ordering::Relaxed) });
        let mut budget = ClobCooperativeBudget::new();
        for expected in 0..1024 {
            budget.checkpoint().await;
            assert_eq!(processed.fetch_add(1, Ordering::Relaxed), expected);
        }
        assert!(sibling.await.unwrap() <= MAX_TURNS as usize);
        assert_eq!(processed.load(Ordering::Relaxed), 1024);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unbounded_ready_loop_reproduces_sibling_starvation() {
        let processed = Arc::new(AtomicUsize::new(0));
        let peer = processed.clone();
        let sibling = tokio::spawn(async move { peer.load(Ordering::Relaxed) });
        for expected in 0..1024 {
            std::future::ready(()).await;
            assert_eq!(processed.fetch_add(1, Ordering::Relaxed), expected);
        }
        assert_eq!(sibling.await.unwrap(), 1024);
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "release benchmark: buffered websocket burst, bounded versus unbounded owner turns"]
    async fn benchmark_buffered_websocket_fairness() {
        use futures_util::StreamExt;
        use std::time::{Duration, Instant};
        use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};
        const BURSTS: usize = 1000;
        const FRAMES: usize = 256;
        let mut encoded = Vec::with_capacity(FRAMES * 6);
        for seq in 0..FRAMES as u32 {
            encoded.extend_from_slice(&[0x82, 4]);
            encoded.extend_from_slice(&seq.to_be_bytes());
        }
        for bounded in [false, true] {
            let mut peers = Vec::with_capacity(BURSTS);
            let mut handlers = Vec::with_capacity(BURSTS * FRAMES);
            let mut bursts = Vec::with_capacity(BURSTS);
            for _ in 0..BURSTS {
                let (socket, _other) = tokio::io::duplex(64);
                // All frames already reside in tungstenite's read buffer.
                // Socket readiness/cooperative budgets cannot limit this loop.
                let mut ws = WebSocketStream::from_partially_read(
                    socket,
                    encoded.clone(),
                    Role::Client,
                    None,
                )
                .await;
                let mut budget = ClobCooperativeBudget::new();
                let started = Instant::now();
                let sibling = tokio::spawn(async move { started.elapsed().as_nanos() as u64 });
                for expected in 0..FRAMES as u32 {
                    if bounded {
                        budget.checkpoint().await;
                    }
                    let start = Instant::now();
                    let bytes = ws.next().await.unwrap().unwrap().into_data();
                    assert_eq!(u32::from_be_bytes(bytes.try_into().unwrap()), expected);
                    // Controlled 4us CPU work models a small frame handler;
                    // it is not a claim about live parser timing.
                    while start.elapsed() < Duration::from_micros(4) {
                        std::hint::spin_loop();
                    }
                    handlers.push(start.elapsed().as_nanos() as u64);
                }
                bursts.push(started.elapsed().as_nanos() as u64);
                peers.push(sibling.await.unwrap());
            }
            for (boundary, values) in [
                ("spawn_to_sibling_first_poll", &mut peers),
                ("buffered_frame_poll_plus_4us_work", &mut handlers),
                ("256_frame_burst", &mut bursts),
            ] {
                values.sort_unstable();
                let at = |q: usize| values[(values.len() * q).div_ceil(1000) - 1];
                eprintln!("clob_fairness bounded={bounded} boundary={boundary} N={} p50_ns={} p99_ns={} p999_ns={} max_ns={} input_buffer_frames={FRAMES} added_queue_depth=0 overflow=0 ordering_errors=0",
                    values.len(), at(500), at(990), at(999), at(1000));
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "release benchmark: kernel deadline readiness during a buffered owner burst"]
    fn benchmark_kernel_readiness_during_burst() {
        use hexagent_runtime::precise_interval::PreciseInterval;
        use std::time::{Duration, Instant};
        for (bounded, event_interval) in [(false, 61), (true, 61), (true, 1)] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .event_interval(event_interval)
                .build()
                .unwrap();
            runtime.block_on(async {
                let mut samples = Vec::with_capacity(1000);
                let mut bursts = Vec::with_capacity(1000);
                for _ in 0..1000 {
                    let mut timer = PreciseInterval::new(Duration::from_micros(200)).unwrap();
                    let observer = tokio::spawn(async move { timer.tick().await.unwrap() });
                    tokio::task::yield_now().await; // register the observer before CPU work
                    let started = Instant::now();
                    let mut budget = ClobCooperativeBudget::new();
                    for _ in 0..256 {
                        if bounded { budget.checkpoint().await; }
                        let work = Instant::now();
                        while work.elapsed() < Duration::from_micros(4) { std::hint::spin_loop(); }
                    }
                    bursts.push(started.elapsed().as_nanos() as u64);
                    samples.push(observer.await.unwrap().lag_ns());
                }
                for (boundary, values) in [("kernel_deadline_to_observer", &mut samples), ("256_turn_burst", &mut bursts)] {
                    values.sort_unstable();
                    let at = |q: usize| values[(values.len() * q).div_ceil(1000) - 1];
                    eprintln!("clob_kernel_fairness bounded={bounded} event_interval={event_interval} boundary={boundary} N={} p50_ns={} p99_ns={} p999_ns={} max_ns={} added_queue_depth=0 overflow=0 modeled_work_us=4",
                        values.len(), at(500), at(990), at(999), at(1000));
                }
            });
        }
    }
}
