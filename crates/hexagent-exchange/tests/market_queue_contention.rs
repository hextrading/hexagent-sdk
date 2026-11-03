//! Offline, bounded-root-lane contention reproducer. No sockets or accounts.
use hexagent_exchange::{
    exchange::{market_audit::AuditWriter, market_event_channel_audited, publish_market_event},
    types::{MarketEvent, SpotPrice},
};
use std::{
    sync::{Arc, Barrier},
    time::Instant,
};
#[test]
#[ignore = "focused contention reproduction/benchmark; set MARKET_AUDIT_OUTPUT to retain stage evidence"]
fn three_producers_below_capacity() {
    const ROUNDS: usize = 500;
    const BURST: usize = 100;
    const PRODUCERS: usize = 3;
    let (sink, mut writer) = match std::env::var("MARKET_AUDIT_OUTPUT") {
        Ok(path) => {
            let (s, w) = AuditWriter::create(std::path::Path::new(&path), 65_536).unwrap();
            (Some(s), Some(w))
        }
        Err(_) => (None, None),
    };
    let (tx, rx) = market_event_channel_audited(10_000, false, sink);
    let barrier = Arc::new(Barrier::new(PRODUCERS + 1));
    let handles: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            let tx = tx.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut samples = Vec::with_capacity(ROUNDS * BURST);
                let mut errors = 0;
                for round in 0..ROUNDS {
                    barrier.wait();
                    for i in 0..BURST {
                        let n = (round * BURST + i + 1) as u64;
                        let e = MarketEvent::SpotPrice(SpotPrice {
                            source: "chainlink".into(),
                            symbol: format!("asset{producer}/usd"),
                            price: n as f64,
                            timestamp_ns: n,
                            local_timestamp_ns: n,
                        });
                        let t = Instant::now();
                        let r = publish_market_event(&tx, e);
                        samples.push(t.elapsed().as_nanos() as u64);
                        errors += usize::from(r.is_err());
                    }
                    barrier.wait();
                }
                (samples, errors)
            })
        })
        .collect();
    let mut received = 0;
    let mut depth = 0;
    for _ in 0..ROUNDS {
        barrier.wait();
        barrier.wait();
        depth = depth.max(tx.consumer_progress().pending);
        while rx.try_recv().is_ok() {
            received += 1;
        }
        if let Some(w) = writer.as_mut() {
            w.drain(65_536).unwrap();
        }
    }
    if let Some(w) = writer.as_mut() {
        w.finish().unwrap();
    }
    let mut ns = Vec::new();
    let mut errors = 0;
    for h in handles {
        let (v, e) = h.join().unwrap();
        ns.extend(v);
        errors += e;
    }
    ns.sort_unstable();
    let n = ns.len();
    let value = serde_json::json!({"sent":n,"received":received,"missing":n-received,"errors":errors,"capacity":10_000,"maximum_possible_depth":PRODUCERS*BURST,"sampled_depth":depth,"median_ns":ns[n/2],"p99_ns":ns[n*99/100],"p999_ns":ns[n*999/1000],"maximum_ns":ns[n-1],"boundary":"owned event to publish return; concurrent producers, consumer drains between bursts; excludes allocations, sockets, strategy and order execution"});
    println!("{value}");
    assert!(depth <= PRODUCERS * BURST);
}
