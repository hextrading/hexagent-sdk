//! Single-owner completion-envelope benchmark (no sockets, no trading).
use std::{
    hint::black_box,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
fn main() {
    const N: usize = 100_000;
    for reused in [false, true, true, false] {
        let mut pool = hexagent_runtime::reply_slots::Pool::new(2, || AtomicU64::new(0), || 0u64);
        let mut samples = Vec::with_capacity(N);
        for n in 0..N + 1000 {
            let started = Instant::now();
            if reused {
                let (tx, rx, timing) = pool.checkout(|t| t.store(0, Ordering::Relaxed)).unwrap();
                tx.try_send(n as u64).unwrap();
                black_box(rx.recv().unwrap());
                drop((rx, timing));
            } else {
                let timing = Arc::new(AtomicU64::new(0));
                let (tx, rx) = crossbeam_channel::bounded(1);
                tx.try_send(n as u64).unwrap();
                black_box(rx.recv().unwrap());
                drop((tx, rx, timing));
            }
            let elapsed = started.elapsed().as_nanos();
            if n >= 1000 {
                samples.push(elapsed);
            }
        }
        samples.sort_unstable();
        println!("completion reused={reused} N={N} boundary=checkout_send_receive_release median_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water=1 overflow=0 pool_capacity=2", samples[N/2], samples[N*99/100-1], samples[N*999/1000-1], samples[N-1]);
    }
}
