#![allow(dead_code)]
#[path = "../../../crates/hexagent-runtime/src/poll_channel.rs"]
mod poll_channel;
#[path = "../../../crates/hexagent-runtime/src/try_queue.rs"]
mod try_queue;
#[path = "../../../crates/hexagent-types/src/types/mod.rs"]
pub mod types;
#[path = "../../../crates/hexagent-runtime/src/wake.rs"]
mod wake;

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

struct Event {
    id: usize,
    receipt: Instant,
    dispatch: u64,
    owner: u64,
    published: u64,
    submitted: u64,
    // Actual fixed-capacity trading envelope, including its causal trace.
    // Empty identity strings avoid per-event allocations in this fixture.
    signal: types::Signal,
}

fn pin(core: usize, priority: i32) {
    #[cfg(target_os = "linux")]
    unsafe {
        assert!(core < libc::CPU_SETSIZE as usize);
        let mut mask: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut mask);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of_val(&mask), &mask),
            0
        );
        if priority > 0 {
            assert_eq!(
                libc::sched_setscheduler(
                    0,
                    libc::SCHED_FIFO,
                    &libc::sched_param {
                        sched_priority: priority
                    }
                ),
                0
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (core, priority);
    }
}
fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos() as u64
}
fn work(us: u64) {
    let start = Instant::now();
    while start.elapsed().as_micros() < us as u128 {
        std::hint::spin_loop();
    }
}
fn queue(waking: bool) -> (poll_channel::Sender<Event>, poll_channel::Receiver<Event>) {
    poll_channel::bounded_with_wake(1024, waking.then(|| Arc::new(wake::Wake::default())))
}
fn summary(
    mode: &str,
    boundary: &str,
    mut samples: Vec<u64>,
    depths: [usize; 3],
    overflows: usize,
    cores: [usize; 3],
    fifo: bool,
    router_us: u64,
    prep_us: u64,
) {
    samples.sort_unstable();
    let n = samples.len();
    println!("{{\"mode\":\"{mode}\",\"boundary\":\"{boundary}\",\"n\":{n},\"median_ns\":{},\"p99_ns\":{},\"p999_ns\":{},\"maximum_ns\":{},\"queue_high_water\":{:?},\"overflow\":{overflows},\"producer_core\":{},\"owner_io_core\":{},\"dispatcher_core\":{},\"fifo\":{fifo},\"synthetic_prep_us\":{prep_us},\"burst\":3,\"router_work_us\":{router_us},\"capacity_per_lane\":1024,\"event_bytes\":{},\"signal_bytes\":{},\"trace_bytes\":{}}}",
        samples[n / 2], samples[(n * 99).div_ceil(100) - 1], samples[(n * 999).div_ceil(1000) - 1], samples[n - 1], depths, cores[0], cores[2], cores[1], std::mem::size_of::<Event>(), std::mem::size_of::<types::Signal>(), std::mem::size_of::<types::HotPathTrace>());
}
fn run(
    mode: &str,
    waking: bool,
    cores: [usize; 3],
    fifo: bool,
    n: usize,
    router_us: u64,
    prep_us: u64,
) {
    let (tx, rx) = queue(waking);
    let (owner_tx, owner_rx) = queue(waking);
    let (io_tx, io_rx) = queue(true);
    let barrier = Arc::new(Barrier::new(4));
    let db = barrier.clone();
    let dispatch = std::thread::spawn(move || {
        pin(cores[1], if fifo { 50 } else { 0 });
        db.wait();
        let mut depth = 0;
        while let Ok(mut e) =
            rx.recv_timeout_with_poll(Duration::from_secs(1), Duration::from_micros(10))
        {
            e.dispatch = ns(e.receipt);
            if let types::Signal::CancelOrder { cancel_trigger, .. } = &mut e.signal {
                cancel_trigger.hot_path.executor_received_mono_ns = types::monotonic_now_ns();
                cancel_trigger.hot_path.owner_published_mono_ns = types::monotonic_now_ns();
            }
            e.published = ns(e.receipt);
            owner_tx.try_send(e).expect("bounded owner queue overflow");
            depth = depth.max(owner_tx.len());
        }
        depth
    });
    let ob = barrier.clone();
    let owner = std::thread::spawn(move || {
        pin(cores[2], if fifo { 50 } else { 0 });
        ob.wait();
        let mut depth = 0;
        while let Ok(mut e) =
            owner_rx.recv_timeout_with_poll(Duration::from_secs(1), Duration::from_micros(50))
        {
            e.owner = ns(e.receipt);
            if let types::Signal::CancelOrder { cancel_trigger, .. } = &mut e.signal {
                cancel_trigger.hot_path.owner_dequeued_mono_ns = types::monotonic_now_ns();
            }
            work(prep_us);
            e.submitted = ns(e.receipt);
            io_tx.try_send(e).expect("bounded I/O queue overflow");
            depth = depth.max(io_tx.len().max(1));
        }
        depth
    });
    let ib = barrier.clone();
    let io = std::thread::spawn(move || {
        pin(cores[2], if fifo { 70 } else { 0 });
        let mut samples = Vec::with_capacity(n);
        ib.wait();
        for expected in 0..n {
            let e = io_rx.recv().unwrap();
            let first_write = ns(e.receipt);
            assert_eq!(e.id, expected, "message lost, duplicated or reordered");
            samples.push([e.dispatch, e.owner - e.published, e.submitted, first_write]);
        }
        samples
    });
    pin(cores[0], if fifo { 60 } else { 0 });
    barrier.wait();
    let mut depth = 0;
    let overflow = 0;
    let mut receipt = Instant::now();
    let mut receipts = types::ReceiptSequencer::new();
    let mut cause = types::MarketReceipt::default();
    for id in 0..n {
        // 3-order bursts use a common causative receive boundary; no feedback
        // wait that would conceal scheduling/backpressure across bursts.
        if id % 3 == 0 {
            std::thread::sleep(Duration::from_millis(1));
            receipt = Instant::now();
            cause = receipts.received().parsed();
        }
        let e = Event {
            id,
            receipt,
            dispatch: 0,
            owner: 0,
            published: 0,
            submitted: 0,
            signal: types::Signal::CancelOrder {
                exchange: types::Exchange::Polymarket,
                instance_id: String::new(),
                client_order_id: String::new(),
                timestamp_ns: 0,
                cancel_trigger: types::CancelTrigger {
                    hot_path: types::HotPathTrace {
                        receipt: cause,
                        clock_domain_ns: types::monotonic_clock_domain_ns(),
                        signal_mono_ns: types::monotonic_now_ns(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
        };
        tx.try_send(e).expect("bounded input overflow");
        depth = depth.max(tx.len());
        if id % 3 == 2 {
            work(router_us);
        }
    }
    drop(tx);
    let depths = [depth, dispatch.join().unwrap(), owner.join().unwrap()];
    let samples = io.join().unwrap();
    for (i, boundary) in [
        "receive_to_dispatch_dequeue",
        "owner_queue_publication_to_dequeue",
        "receive_to_http_submit",
        "receive_to_first_write_stub",
    ]
    .iter()
    .enumerate()
    {
        summary(
            mode,
            boundary,
            samples.iter().map(|row| row[i]).collect(),
            depths,
            overflow,
            cores,
            fifo,
            router_us,
            prep_us,
        );
    }
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let base: usize = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(0);
    let isolated: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(1);
    let n: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(30_000);
    let fifo = args.get(4).is_some_and(|s| s == "fifo");
    let owner: usize = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(base);
    let router_us: u64 = args.get(6).map(|s| s.parse().unwrap()).unwrap_or(0);
    assert!(router_us <= 200);
    let prep_us: u64 = args.get(7).map(|s| s.parse().unwrap()).unwrap_or(20);
    assert!(prep_us <= 60);
    assert!(n >= 1000 && n <= 300_000);
    for (mode, waking, dc) in [
        ("poll_shared", false, base),
        ("wake_shared", true, base),
        ("poll_isolated", false, isolated),
        ("wake_isolated", true, isolated),
    ] {
        run(mode, waking, [base, dc, owner], fifo, n, router_us, prep_us);
    }
}
