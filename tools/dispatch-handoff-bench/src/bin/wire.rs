#![allow(dead_code)]
#[path = "../wire_baseline.rs"]
mod baseline;
#[path = "../wire_candidate.rs"]
mod candidate;
use std::{
    alloc::{GlobalAlloc, Layout},
    hint::black_box,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};
struct CountAlloc;
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
#[global_allocator]
static ALLOC: CountAlloc = CountAlloc;
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        mimalloc::MiMalloc.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        mimalloc::MiMalloc.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        mimalloc::MiMalloc.realloc(p, l, size)
    }
}
const ADDRESS: &str = "0x1234567890123456789012345678901234567890";
const OWNER: &str = "00000000-0000-0000-0000-000000000000";
const TOKEN: &str = "50303916472381649224674364401111317755258653723694532482715411789597335197187";
const ZERO: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";
const SIGNATURE: &str = "0x000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
fn main() {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut mask: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(0, &mut mask);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask),
            0
        );
    }
    const N: usize = 100_000;
    let mut baseline_json = Vec::new();
    let mut candidate_json = Vec::new();
    for borrowed in [false, true] {
        let mut samples = Vec::with_capacity(N);
        let mut allocations = Vec::with_capacity(N);
        let mut buffer = Vec::with_capacity(2048);
        for id in 0..N {
            buffer.clear();
            ALLOCATIONS.store(0, Ordering::Relaxed);
            let start = Instant::now();
            if borrowed {
                let body = candidate::WireBodyV2 {
                    owner: OWNER,
                    order_type: "GTC",
                    post_only: true,
                    defer_exec: false,
                    order: candidate::WireOrderV2 {
                        salt: 7,
                        maker: ADDRESS,
                        signer: ADDRESS,
                        taker: ADDRESS,
                        token_id: TOKEN,
                        maker_amount: 10,
                        taker_amount: 20,
                        side: "BUY",
                        signature_type: 3,
                        timestamp: 1791000000000,
                        expiration: "0",
                        metadata: ZERO,
                        builder: ZERO,
                        signature: SIGNATURE.to_owned(),
                    },
                };
                serde_json::to_writer(&mut buffer, black_box(&body)).unwrap();
                drop(body);
            } else {
                let body = baseline::WireBodyV2 {
                    owner: OWNER.to_owned(),
                    order_type: "GTC",
                    post_only: true,
                    defer_exec: false,
                    order: baseline::WireOrderV2 {
                        salt: 7,
                        maker: ADDRESS.to_owned(),
                        signer: ADDRESS.to_owned(),
                        taker: ADDRESS.to_owned(),
                        token_id: TOKEN.to_owned(),
                        maker_amount: 10,
                        taker_amount: 20,
                        side: "BUY",
                        signature_type: 3,
                        timestamp: 1791000000000,
                        expiration: "0".to_owned(),
                        metadata: ZERO.to_owned(),
                        builder: ZERO.to_owned(),
                        signature: SIGNATURE.to_owned(),
                    },
                };
                serde_json::to_writer(&mut buffer, black_box(&body)).unwrap();
                drop(body);
            }
            let elapsed = start.elapsed().as_nanos() as u64;
            allocations.push(ALLOCATIONS.load(Ordering::Relaxed));
            samples.push(elapsed);
            if id == 0 {
                if borrowed {
                    candidate_json = buffer.clone();
                } else {
                    baseline_json = buffer.clone();
                }
            }
        }
        samples.sort_unstable();
        allocations.sort_unstable();
        println!("{{\"borrowed\":{borrowed},\"boundary\":\"wire_construct_serialize_drop\",\"n\":{N},\"median_ns\":{},\"p99_ns\":{},\"p999_ns\":{},\"maximum_ns\":{},\"allocations_per_event\":{},\"queue_depth\":0,\"overflow\":0,\"allocator\":\"mimalloc_counted\"}}",samples[N/2],samples[N*99/100-1],samples[N*999/1000-1],samples[N-1],allocations[N/2]);
    }
    assert_eq!(baseline_json, candidate_json, "wire JSON changed");
}
