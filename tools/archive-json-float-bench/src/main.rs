use serde::Deserialize;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    hint::black_box,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};
struct Count;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Count {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(ptr, layout, size)
    }
}
#[global_allocator]
static A: Count = Count;
#[derive(Deserialize)]
struct Rtds<'a> {
    topic: &'a str,
    #[serde(borrow)]
    payload: Payload<'a>,
    timestamp: u64,
}
#[derive(Deserialize)]
struct Payload<'a> {
    symbol: &'a str,
    value: f64,
}
fn bench(label: &str, mut run: impl FnMut()) {
    for _ in 0..10_000 {
        run();
    }
    let mut samples = Vec::with_capacity(100_000);
    let before = ALLOCS.load(Ordering::Relaxed);
    for _ in 0..100_000 {
        let start = Instant::now();
        run();
        samples.push(start.elapsed().as_nanos());
    }
    let allocations = ALLOCS.load(Ordering::Relaxed) - before;
    samples.sort_unstable();
    println!("mode={} case={} n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} allocations={} queue_depth=0 overflow=0 boundary=JSON_decode_and_drop_only excludes=network_queue_strategy_dispatch", if cfg!(feature="precise") {"precise"} else {"default"},label,samples.len(),samples[49_999],samples[98_999],samples[99_899],samples[99_999],allocations);
}
fn main() {
    let x: f64 = serde_json::from_str("0.040000000000000036").unwrap();
    println!(
        "repro bits={:016x} expected={:016x} exact={}",
        x.to_bits(),
        0.040000000000000036f64.to_bits(),
        x.to_bits() == 0.040000000000000036f64.to_bits()
    );
    let rtds=br#"{"topic":"crypto_prices","payload":{"symbol":"btcusdt","value":61234.56789012345},"timestamp":1757908892351}"#;
    bench("borrowed_rtds_numeric", || {
        let row: Rtds = serde_json::from_slice(black_box(rtds)).unwrap();
        black_box((
            row.topic,
            row.payload.symbol,
            row.payload.value,
            row.timestamp,
        ));
    });
    bench("actual_archive_proof_value", || {
        black_box(
            serde_json::from_slice::<serde_json::Value>(black_box(include_bytes!("../proof.json")))
                .unwrap(),
        );
    });
}
