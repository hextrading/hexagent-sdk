//! Repeated cancel-only callbacks while POST acknowledgements are outstanding.
//! Run the same example before/after an OM change. No network or worker threads.
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use hexagent_account::account::order_manager::{OrderManager, ReconcileAction};
use hexagent_account::types::{Exchange, OrderType, Side};

struct CountAlloc;
static TRACK: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if TRACK.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if TRACK.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        System.realloc(ptr, layout, size)
    }
}
#[global_allocator]
static ALLOCATOR: CountAlloc = CountAlloc;

fn cancel(manager: &mut OrderManager) {
    manager
        .cancel_all_with(black_box(100), |_| -> Result<(), ()> {
            panic!("a parked Submitted order must not emit DELETE before ACK")
        })
        .unwrap();
}

fn main() {
    const N: usize = 100_000;
    let require_no_alloc = std::env::args().any(|arg| arg == "--require-no-alloc");
    println!("pending,events,median_ns,p99_ns,p999_ns,max_ns,allocations,queue_depth,overflow");
    for pending in [0, 1, 8, 32] {
        let mut manager =
            OrderManager::new(Exchange::Polymarket, "TOKEN".into(), 0.01, "bench".into());
        for i in 0..pending {
            manager.apply_reconcile_with(
                &[ReconcileAction::Place {
                    side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                    price: 0.4,
                    quantity: 5.0,
                    order_type: OrderType::Limit,
                    post_only: true,
                }],
                1,
                |_| {},
            );
        }
        cancel(&mut manager);
        for _ in 0..1_000 {
            cancel(&mut manager);
        }
        let mut samples = Vec::with_capacity(N);
        ALLOCS.store(0, Ordering::Relaxed);
        for _ in 0..N {
            TRACK.store(true, Ordering::Relaxed);
            let start = Instant::now();
            cancel(black_box(&mut manager));
            let elapsed = start.elapsed().as_nanos();
            TRACK.store(false, Ordering::Relaxed);
            samples.push(elapsed);
        }
        let allocations = ALLOCS.load(Ordering::Relaxed);
        if require_no_alloc {
            assert_eq!(allocations, 0, "steady cancel pause allocated");
        }
        samples.sort_unstable();
        println!(
            "{pending},{N},{},{},{},{},{allocations},0,0",
            samples[N / 2],
            samples[N * 99 / 100],
            samples[N * 999 / 1000],
            samples[N - 1]
        );
        assert_eq!(manager.cancel_before_ack_count(), pending);
    }
}
