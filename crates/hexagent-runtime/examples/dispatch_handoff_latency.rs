//! Synthetic remote handoff benchmark: no sockets, auth, trading or account state.
//! `BENCH_N`, `BENCH_PRODUCER_CPU`, `BENCH_CONSUMER_CPU` control sampling/pinning.
//! Reports publication wall time, publication-to-consumption, producer allocations,
//! exact in-flight high water and retained/retried overflow. Run optimized.
use hexagent_runtime::{poll_channel, root_owner, try_queue::TryQueue, wake::Wake};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::Poll,
    time::{Duration, Instant},
};

struct CountAlloc;
thread_local! { static COUNT: Cell<Option<usize>> = const { Cell::new(None) }; }
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        COUNT.with(|c| {
            if let Some(n) = c.get() {
                c.set(Some(n + 1));
            }
        });
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout)
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        COUNT.with(|c| {
            if let Some(n) = c.get() {
                c.set(Some(n + 1));
            }
        });
        System.realloc(p, layout, size)
    }
}
#[global_allocator]
static ALLOC: CountAlloc = CountAlloc;
fn pin(name: &str) {
    if let Ok(cpu) = std::env::var(name) {
        #[cfg(target_os = "linux")]
        unsafe {
            let mut cpus: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(cpu.parse().unwrap(), &mut cpus);
            assert_eq!(
                libc::sched_setaffinity(0, std::mem::size_of_val(&cpus), &cpus),
                0
            );
        }
        #[cfg(not(target_os = "linux"))]
        let _ = cpu;
    }
}
fn summary(
    lane: &str,
    boundary: &str,
    samples: &mut [u64],
    allocations: usize,
    high: usize,
    overflow: usize,
) {
    samples.sort_unstable();
    let n = samples.len();
    println!("lane={lane} boundary={boundary} n={n} median_ns={} p99_ns={} p999_ns={} max_ns={} producer_allocations={allocations} inflight_high_water={high} overflow_retries={overflow} drops=0", samples[n/2], samples[(n*99).div_ceil(100)-1], samples[(n*999).div_ceil(1000)-1], samples[n-1]);
}
fn io_handoff(n: usize, actors: bool, burst: usize) {
    let (registry, mut driver) = root_owner::driver(256);
    let (handles, receive_handle) = std::sync::mpsc::sync_channel(1);
    let (shutdown, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let worker = std::thread::spawn(move || {
        pin("BENCH_CONSUMER_CPU");
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        handles.send(rt.handle().clone()).unwrap();
        rt.block_on(futures_util::future::poll_fn(|cx| {
            if Pin::new(&mut shutdown_rx).poll(cx).is_ready() {
                Poll::Ready(())
            } else {
                Pin::new(&mut driver).poll(cx)
            }
        }));
    });
    let handle = receive_handle.recv().unwrap();
    let replies = Arc::new(TryQueue::new(64));
    let mut mailboxes = Vec::new();
    if actors {
        for _ in 0..24 {
            let replies = replies.clone();
            mailboxes.push(
                registry
                    .register(2, |mut rx: root_owner::Inbox<Instant>| async move {
                        while let Some(sent) = rx.recv().await {
                            let elapsed = sent.elapsed().as_nanos() as u64;
                            assert!(replies.try_push(elapsed).is_ok());
                        }
                    })
                    .unwrap(),
            );
        }
    }
    let mut publish = Vec::with_capacity(n);
    let mut consume = Vec::with_capacity(n);
    let mut allocations = 0;
    let mut overflow = 0;
    let mut high = 0;
    let warm = 1200;
    let mut index = 0;
    while index < n + warm {
        let count = burst.min(n + warm - index);
        high = high.max(count);
        for offset in 0..count {
            let measured = index + offset >= warm;
            COUNT.with(|c| c.set(Some(0)));
            let sent = Instant::now();
            if actors {
                let tx = &mailboxes[(index + offset) % mailboxes.len()];
                let mut value = sent;
                loop {
                    match tx.try_send(value) {
                        Ok(()) => break,
                        Err(crossbeam_channel::TrySendError::Full(retained)) => {
                            overflow += 1;
                            value = retained;
                            std::thread::yield_now();
                        }
                        Err(_) => panic!("actor stopped"),
                    }
                }
            } else {
                let out = replies.clone();
                handle.spawn(async move {
                    let elapsed = sent.elapsed().as_nanos() as u64;
                    assert!(out.try_push(elapsed).is_ok());
                });
            }
            let elapsed = sent.elapsed().as_nanos() as u64;
            let allocated = COUNT.with(|c| c.replace(None).unwrap());
            if measured {
                publish.push(elapsed);
                allocations += allocated;
            }
        }
        for offset in 0..count {
            let elapsed = loop {
                if let Some(reply) = replies.try_pop() {
                    break reply;
                }
                std::hint::spin_loop();
            };
            if index + offset >= warm {
                consume.push(elapsed);
            }
        }
        index += count;
    }
    let lane = format!(
        "{}-burst{burst}",
        if actors { "root_owner" } else { "tokio_spawn" }
    );
    summary(
        &lane,
        "before_publish_to_return",
        &mut publish,
        allocations,
        high,
        overflow,
    );
    summary(
        &lane,
        "before_publish_to_consume",
        &mut consume,
        allocations,
        high,
        overflow,
    );
    shutdown.send(()).unwrap();
    worker.join().unwrap();
}
fn idle_wake(n: usize, polling: bool) {
    let wake = Arc::new(Wake::default());
    let (ptx, prx) = poll_channel::bounded_with_wake(1, Some(wake));
    let (ctx, crx) = crossbeam_channel::bounded::<Instant>(1);
    let replies = Arc::new(TryQueue::new(2));
    let out = replies.clone();
    let armed = Arc::new(AtomicBool::new(false));
    let ready = armed.clone();
    let warm = 100;
    let worker = std::thread::spawn(move || {
        pin("BENCH_CONSUMER_CPU");
        let disabled = crossbeam_channel::never::<Instant>();
        for _ in 0..n + warm {
            ready.store(true, Ordering::Release);
            let sent = if polling {
                prx.recv().unwrap()
            } else {
                crossbeam_channel::select_biased! {
                    recv(crx) -> msg => msg.unwrap(),
                    recv(disabled) -> msg => msg.unwrap(),
                    recv(disabled) -> msg => msg.unwrap(),
                    recv(disabled) -> msg => msg.unwrap(),
                    recv(disabled) -> msg => msg.unwrap(),
                }
            };
            assert!(out.try_push(sent.elapsed().as_nanos() as u64).is_ok());
        }
    });
    let mut publish = Vec::with_capacity(n);
    let mut consume = Vec::with_capacity(n);
    let mut allocations = 0;
    for i in 0..n + warm {
        while !armed.swap(false, Ordering::AcqRel) {
            std::hint::spin_loop();
        }
        // Give the consumer an idle interval to register/park. This is outside
        // both measured boundaries and identical for old and new paths.
        std::thread::sleep(Duration::from_micros(50));
        COUNT.with(|c| c.set(Some(0)));
        let sent = Instant::now();
        if polling {
            ptx.try_send(sent).unwrap();
        } else {
            ctx.try_send(sent).unwrap();
        }
        let elapsed = sent.elapsed().as_nanos() as u64;
        let allocated = COUNT.with(|c| c.replace(None).unwrap());
        let receipt = loop {
            if let Some(elapsed) = replies.try_pop() {
                break elapsed;
            }
            std::hint::spin_loop();
        };
        if i >= warm {
            publish.push(elapsed);
            consume.push(receipt);
            allocations += allocated;
        }
    }
    worker.join().unwrap();
    let lane = if polling {
        "futex_idle"
    } else {
        "crossbeam_select_idle"
    };
    summary(
        lane,
        "before_publish_to_return",
        &mut publish,
        allocations,
        1,
        0,
    );
    summary(
        lane,
        "before_publish_to_consume",
        &mut consume,
        allocations,
        1,
        0,
    );
}
fn main() {
    pin("BENCH_PRODUCER_CPU");
    let n = std::env::var("BENCH_N")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(100_000);
    for actors in [false, true, true, false] {
        for burst in [1, 6] {
            io_handoff(n, actors, burst);
        }
    }
    for polling in [false, true, true, false] {
        idle_wake(n.min(20_000), polling);
    }
}
