//! Ignored, loopback-only complete cancel path. No venue credentials or requests.
//! The runner supplies a private network namespace and checks CPU availability.
//! Fast CPU work and router work are explicitly synthetic; cancel HTTP parsing,
//! handler, health maintenance, command publication and diagnostics are actual.
use super::*;
use hexagent_runtime::offline_delivery_probe as delivery_probe;
use delivery_probe::{Phase, Span};
use std::io::{BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};

fn ns() -> u64 { crate::types::monotonic_now_ns() }
fn role(name: &str, core: usize, priority: u8) {
    crate::os_tune::pin_current(core, name);
    if priority > 0 { crate::os_tune::set_fifo(priority, name); }
}
fn burn_cpu(us: u64) {
    if us == 0 { return; }
    let until = crate::latency::thread_cpu_ns() + us * 1000;
    while crate::latency::thread_cpu_ns() < until { std::hint::spin_loop(); }
}

// The old fixture's Crossbeam blocking recv introduces yield/backoff that is
// absent from production LiveSignalIngress. Retain it as an explicit ablation,
// while the matched primary fixture calls the real ingress and idle policy.
enum RouterInputTx { Legacy(Sender<u64>), Live(SignalTx) }
enum RouterInputRx { Legacy(Receiver<u64>), Live(ExecutionSignalIngress) }
impl RouterInputTx {
    fn publish(&self, start: u64) {
        match self {
            Self::Legacy(tx) => tx.try_send(start).unwrap(),
            Self::Live(tx) => tx.try_send(RoutedSignal { owner: 0, signal: Signal::CancelOrder {
                cancel_trigger: Default::default(), exchange: Exchange::Polymarket,
                client_order_id: String::new(), instance_id: String::new(), timestamp_ns: start,
            }}).unwrap(),
        }
    }
}
impl RouterInputRx {
    fn recv(&mut self) -> Option<u64> {
        match self {
            Self::Legacy(rx) => rx.recv().ok(),
            Self::Live(rx) => loop {
                let value = match rx.try_recv() {
                    Err(crossbeam_channel::TryRecvError::Empty) => rx.idle_recv(),
                    other => other,
                };
                match value {
                    Ok(RoutedSignal { owner: 0, signal: Signal::CancelOrder { timestamp_ns, .. } }) => return Some(timestamp_ns),
                    Ok(_) => panic!("offline ingress identity changed"),
                    Err(crossbeam_channel::TryRecvError::Disconnected) => return None,
                    Err(crossbeam_channel::TryRecvError::Empty) => {},
                }
            },
        }
    }
}
fn router_input(legacy: bool) -> (RouterInputTx, RouterInputRx) {
    if legacy {
        let (tx,rx)=bounded(1);return (RouterInputTx::Legacy(tx),RouterInputRx::Legacy(rx));
    }
    let (tx,rx)=poll_channel::bounded_with_wake(1,Some(Arc::new(Wake::default())));
    let (control,control_rx)=poll_channel::bounded(1);drop(control);
    (RouterInputTx::Live(tx.into()),RouterInputRx::Live(ExecutionSignalIngress::Live(
        LiveSignalIngress::new(rx,Vec::new(),control_rx))))
}

// Format real diagnostic log records on their existing background owner, with
// bounded scratch and no disk in the measured interval. This is not disk I/O.
struct FormatSink;
static FORMATTED: AtomicU64 = AtomicU64::new(0);
impl log::Log for FormatSink {
    fn enabled(&self, m: &log::Metadata) -> bool { m.level() <= log::Level::Info }
    fn log(&self, r: &log::Record) {
        if !self.enabled(r.metadata()) { return; }
        struct Count(usize);
        impl std::fmt::Write for Count {
            fn write_str(&mut self, s: &str) -> std::fmt::Result { self.0 += s.len(); Ok(()) }
        }
        let mut sink = Count(0);
        std::fmt::write(&mut sink, *r.args()).unwrap();
        FORMATTED.fetch_add(sink.0 as u64, Ordering::Relaxed);
    }
    fn flush(&self) {}
}

fn serve(mut stream: TcpStream, stopped: Arc<AtomicBool>, core: usize,
         replies: Arc<AtomicU64>, delay_us: u64) {
    role("off-http-peer", core, 0);
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    let mut input = std::io::BufReader::new(stream.try_clone().unwrap());
    while !stopped.load(Ordering::Acquire) {
        let mut line = String::new();
        match input.read_line(&mut line) {
            Ok(0) => break,
            Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => continue,
            Err(_) => break,
            Ok(_) => {}
        }
        let delete = line.starts_with("DELETE /order ");
        let mut len = 0;
        loop {
            line.clear(); input.read_line(&mut line).unwrap();
            if line == "\r\n" { break; }
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") { len = value.trim().parse().unwrap(); }
            }
        }
        let mut body = vec![0; len]; input.read_exact(&mut body).unwrap();
        let response = if delete {
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let oid = value["orderID"].as_str().expect("real DELETE payload");
            if delay_us > 0 { thread::sleep(Duration::from_micros(delay_us)); }
            replies.fetch_add(1, Ordering::Relaxed);
            format!("{{\"canceled\":[\"{oid}\"],\"not_canceled\":{{}}}}")
        } else { "{}".to_string() };
        if write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}", response.len(), response).is_err() { break; }
    }
}

#[test]
#[ignore = "actual loopback HTTP/FIFO benchmark; use guarded offline runner"]
fn actual_http_reactor() {
    use hexagent_runtime::http1_pool::{self, Role};
    use crate::exchange::polymarket::trade::{ClobVersion, GapReplayConfig, HttpOrderLane};
    use crate::exchange::polymarket::signer::SignatureType;
    let out = std::env::var("HEX_ACTUAL_OUT").expect("explicit evidence output required");
    let waves: usize = std::env::var("HEX_ACTUAL_WAVES").unwrap_or("100".into()).parse().unwrap();
    let budget: usize = std::env::var("HEX_ACTUAL_BATCH").unwrap_or("4".into()).parse().unwrap();
    let cores: usize = std::env::var("HEX_ACTUAL_CORES").unwrap_or("0".into()).parse().unwrap();
    let router_us: u64 = std::env::var("HEX_ACTUAL_ROUTER_US").unwrap_or("200".into()).parse().unwrap();
    let idle_every: usize = std::env::var("HEX_ACTUAL_IDLE_EVERY").unwrap_or("40".into()).parse().unwrap();
    let delay_us: u64 = std::env::var("HEX_ACTUAL_HTTP_US").unwrap_or("0".into()).parse().unwrap();
    assert!(waves > 0 && waves <= 20000 && budget <= 4 && matches!(cores, 0 | 4 | 16));
    assert!(router_us <= 200 && delay_us <= 1000);
    let probing = std::env::var("HEX_ACTUAL_DELIVERY_PROBE").as_deref() == Ok("1");
    const LANES: usize = 6;
    const WARM: usize = 100;
    let (owner_core, dispatcher_core, fast_cores, io_core, cold_core) = if cores == 16 {
        (4, 15, [5, 14], 2, 0)
    } else { (0, 1, [2, 3], 0, 1) };
    let mut tune = hexagent_config::config::OsTuneConfig::default();
    tune.enable_pin = cores != 0; tune.enable_fifo = cores != 0;
    tune.execution_wakeup = true;
    tune.async_rt_core = Some(if cores == 16 {3} else {1});
    tune.async_ord_core = Some(io_core); tune.async_clob_core = Some(if cores == 16 {8} else {1});
    tune.cold_connect_core = Some(cold_core);
    tune.execution_core = Some(dispatcher_core); tune.strategy_core = Some(owner_core);
    tune.poly_exec_cores = fast_cores.to_vec(); tune.poly_cancel_cores = vec![owner_core];
    tune.poly_completion_cores = vec![owner_core]; tune.background_cores = vec![owner_core];
    tune.fifo_async_rt = Some(70); tune.fifo_strategy = Some(60);
    tune.fifo_execution = Some(50); tune.fifo_cancel = Some(55); tune.fifo_completion = Some(55);
    let legacy_private = std::env::var("HEX_ACTUAL_LEGACY_PRIVATE_OWNER").as_deref() == Ok("1");
    if !legacy_private {
        tune.private_owner_cores.insert("offline".into(), if cores == 16 {10} else {0});
        tune.private_cold_cores.insert("offline".into(), owner_core);
        tune.fifo_private_apply = Some(59);
    }
    crate::os_tune::init_from_config(&tune);
    log::set_logger(&FormatSink).unwrap(); log::set_max_level(log::LevelFilter::Info);
    hexagent_runtime::async_rt::init().unwrap();
    role("off-fixture", dispatcher_core, 0);
    let stop = Arc::new(AtomicBool::new(false));
    let replies = Arc::new(AtomicU64::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url: Arc<str> = format!("http://{}", listener.local_addr().unwrap()).into();
    let s = stop.clone(); let count = replies.clone();
    let server = thread::spawn(move || {
        role("off-http-listen", cold_core, 0);
        let mut connections = Vec::new();
        while !s.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => { let s=s.clone(); let c=count.clone(); connections.push(thread::spawn(move || serve(stream,s,cold_core,c,delay_us))); }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(1)),
                Err(e) => panic!("offline accept: {e}"),
            }
        }
        for h in connections { h.join().unwrap(); }
    });
    let shutdown = ShutdownToken::new();
    let seed = PolymarketTrade::new_with_pool_for_startup_query_repair_and_shutdown(
        "offline", "c2VjcmV0", "offline", "0x0000000000000000000000000000000000000000000000000000000000000001",
        false, 100000, SignatureType::Eoa, ClobVersion::V2, "", &url, true,
        "offline", "", GapReplayConfig::default(), None, shutdown.clone()).unwrap();
    let shared = seed.shared_state();
    assert_eq!(shared.offline_unified_private_owner(), !legacy_private);
    hexagent_runtime::async_rt::block_on_order_runtime(async move {
        delivery_probe::prepare(300, (waves + WARM) * LANES * 64, probing);
    });
    http1_pool::init_account_pools(&HashMap::from([("offline".into(), vec!["a".into(), "b".into()])])).unwrap();
    let config: Config = toml::from_str("[general]\nmode='live'").unwrap();
    let states = HashMap::from([("a".to_string(), shared.clone())]);
    let (diag_tx, diag_rx) = poll_channel::bounded(EXECUTION_DIAGNOSTIC_CAPACITY);
    let (pub_tx, pub_rx) = poll_channel::bounded(EXECUTION_DIAGNOSTIC_CAPACITY);
    let (diagnostics, slow_registration) = spawn_execution_diagnostics(diag_rx, pub_rx).unwrap();
    let (updates, received) = account_completion_channel(LANES);
    let completion_backend = received.backend();
    let completion_wait = received.wait_kind();
    let control_wait = if budget == 0 { "not-used" } else { control_wake().offline_wait_kind() };
    let mut metrics = Vec::new(); let mut lanes = Vec::new(); let mut health = Vec::new();
    let mut owners = Vec::new(); let mut handles = Vec::new();
    let reactor_wake = new_reactor_wake();
    let reactor_wait = if budget == 0 { "not-used" } else { reactor_wake.offline_wait_kind() };
    for slot in 0..LANES {
        let wake = if budget == 0 { Arc::new(Wake::default()) } else { reactor_wake.clone() };
        let permit = http1_pool::take_account_execution_slot("offline", Role::Cancel, slot + 1).unwrap();
        let (tx, rx) = poll_channel::bounded_with_wake(1, Some(wake.clone()));
        let mut m = PolyConnectionLaneMetrics::new("offline", Role::Cancel, slot+1);
        m.diagnostic = Some(diag_tx.clone()); m.publication = Some(pub_tx.clone());
        m.register_slow_diagnostic(&slow_registration); let m = Arc::new(m);
        lanes.push(PolyConnectionLane{tx,metrics:m.clone(),preparation:None}); metrics.push(m.clone());
        let (publisher, consumer) = snapshot_lane(); health.push(consumer);
        let mut router = LiveRouter::new_with_poly_map(&config, &states);
        router.poly_route_mut("a").unwrap().bind_http_order_lane(HttpOrderLane::new().unwrap());
        let u=url.clone();
        if budget == 0 {
            handles.push(thread::spawn(move || { role("off-cancel-old",owner_core,55);
                delivery_probe::prepare(slot as u16 + 1, (waves+WARM)*12, probing);
                run_poly_connection_owner(router,permit,Role::Cancel,rx,m,Some(publisher),None,u);
                delivery_probe::take() }));
        } else {
            let binding = crate::os_tune::reserve_execution_binding("off-control", crate::os_tune::ExecutionThreadRole::PolymarketCancel);
            owners.push(Owner::new(router,permit,rx,m,Some(publisher),u,wake,binding,format!("off-control-{slot}")));
        }
    }
    drop(slow_registration);
    if budget != 0 { handles.push(thread::spawn(move || { role("off-reactor",owner_core,55);
        delivery_probe::prepare(100, (waves+WARM)*LANES*6, probing);
        run(owners,reactor_wake,budget); delivery_probe::take() })); }
    let legacy_router = std::env::var("HEX_ACTUAL_LEGACY_ROUTER").as_deref() == Ok("1");
    let (router_tx, mut router_rx) = router_input(legacy_router); let (router_done, routed) = bounded(1);
    let router = thread::spawn(move || { role("off-router",owner_core,60);
        delivery_probe::prepare(90, waves+WARM, probing);
        while let Some(start) = router_rx.recv() {
            let began=ns(); let probe=Span::begin_at(Phase::Router,start,began); burn_cpu(router_us); let end=ns();
            if let Some(p)=probe {p.finish_at(end,0,None,0);}
            router_done.send((start,began,end)).unwrap();
        } delivery_probe::take() });
    let (fast_done, fast_received) = bounded(LANES);
    let mut fast_txs=Vec::new(); let mut fast_handles=Vec::new();
    let fast_wait = physical_owner_wake(Role::Fast).offline_wait_kind();
    for slot in 0..LANES {
        let core = fast_cores[slot % 2];
        let wake = Arc::new(physical_owner_wake(Role::Fast));
        let hint = hexagent_runtime::wake::StateNotification::default();
        hint.bind(wake.clone());
        let (tx,rx)=poll_channel::bounded_with_wake::<u64>(1,Some(wake));fast_txs.push(tx);
        let done=fast_done.clone();fast_handles.push(thread::spawn(move || { role("off-fast",core,50);
            delivery_probe::prepare(200+slot as u16,(waves+WARM)*24,probing);
            loop {
                let mut resumed = 0;
                let start = match rx.recv_timeout_observed_interruptible(
                    HEARTBEAT, Duration::from_micros(50), &mut resumed, || hint.take_pending()) {
                    Ok(start) => start,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                };
                let began=ns();let probe=Span::begin_at(Phase::Fast,start,began);
                burn_cpu(60);let end=ns();if let Some(p)=probe {p.finish_at(end,0,None,0);}
                done.send((start,began,end)).unwrap(); }
            delivery_probe::take()
        }));
    }
    let mut rows=Vec::with_capacity(waves*LANES);
    let mut health_rows=Vec::with_capacity((waves+WARM)*LANES);
    let coids:Vec<_>=(0..LANES).map(|i|format!("offline-{i}")).collect();
    let mut diag_hwm=0;let mut update_hwm=0;let mut missed=0;
    let mut deadline=Instant::now();
    let mut idle_heartbeats = 0;
    // No privileged pin/fifo on developer laptops; Linux topology is supplied
    // only by the guarded runner. Setup/identity replay are outside boundaries.
    role("off-dispatch",dispatcher_core,50);
    delivery_probe::prepare(0,(waves+WARM)*LANES*5,probing);
    for wave in 0..waves+WARM {
        if idle_every > 0 && wave >= WARM && (wave-WARM)%idle_every == 0 {
            // Real idle heartbeat expiration, not a synthetic maintenance burn.
            // Observe two full periods: the existing deadline keeps its phase,
            // so the first heartbeat can legitimately arrive immediately.
            // Success notifications during continuous traffic can otherwise
            // postpone every 100ms deadline and hide this workload entirely.
            let idle_start = ns();
            thread::sleep(Duration::from_millis(220));
            for (slot, rx) in health.iter().enumerate() {
                let o = rx.try_recv().expect("100ms idle maintenance must publish");
                assert!(o.observed_mono_ns >= idle_start + 100_000_000,
                    "stale pre-idle snapshot is not proof of a heartbeat");
                health_rows.push((wave,slot,o.sequence,o.observed_mono_ns,ns(),o.health.pool_generation,o.health.quarantined));
                idle_heartbeats += 1;
            }
            deadline = Instant::now()+Duration::from_millis(1);
        }
        for slot in 0..LANES { shared.offline_seed_cancel(&coids[slot], &format!("0x{wave:016x}{slot:016x}")); }
        if Instant::now() < deadline { thread::sleep(deadline-Instant::now()); } else if wave>0 {missed+=1;}
        deadline = Instant::now()+Duration::from_millis(5);
        let input=ns();router_tx.publish(input);
        let fast_start=ns();for tx in &fast_txs {tx.try_send(fast_start).unwrap();}
        let mut send_times=[(0,0);LANES];
        for slot in 0..LANES {
            let mut command=Some(PolyConnectionCommand::Cancel { cancel_trigger:Default::default(),instance_id:"a".into(),exchange:Exchange::Polymarket,
                client_order_id:coids[slot].clone(),timestamp_ns:now_ns(),update_tx:ExecutorUpdateSender{owner:slot as u16,tx:updates.clone().into()},enqueued_at:Instant::now() });
            let started=ns();let probe=Span::begin_at(Phase::FullSend,(wave*16+slot) as u64,started);
            lanes[slot].try_send_slot(&mut command).unwrap();let end=ns();send_times[slot]=(started,end);
            if let Some(p)=probe {p.finish_at(end,0,None,0);}
        }
        diag_hwm=diag_hwm.max(pub_tx.len());
        let mut delivered=[0;LANES];
        for _ in 0..LANES {
            update_hwm=update_hwm.max(received.len());
            let probe=Span::begin_id(Phase::Receive,0);
            let update=received.recv().unwrap();let at=if probing {ns()} else {0};let slot=update.owner as usize;
            if let Some(p)=probe {p.finish_at(at,0,delivery_probe::key(update.update.exchange_order_id.as_deref()),0);}
            assert!(slot<LANES && delivered[slot]==0);assert_eq!(update.update.client_order_id,coids[slot]);
            assert_eq!(update.update.status,OrderStatus::Cancelled);delivered[slot]=ns();
        }
        let (router_input,router_start,router_end)=routed.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(router_input,input);
        let mut fast_end=fast_start;let mut fast_wait=0;
        for _ in 0..LANES {let(start,began,end)=fast_received.recv_timeout(Duration::from_secs(2)).unwrap();assert_eq!(start,fast_start);fast_end=fast_end.max(end);fast_wait=fast_wait.max(began-start);}
        for (slot,rx) in health.iter().enumerate() {if let Ok(o)=rx.try_recv(){health_rows.push((wave,slot,o.sequence,o.observed_mono_ns,ns(),o.health.pool_generation,o.health.quarantined));}}
        if wave>=WARM {for slot in 0..LANES {rows.push((wave,slot,input,send_times[slot].0,send_times[slot].1,delivered[slot],router_start,router_end,fast_start,fast_end,fast_wait));}}
        let limit=Instant::now()+Duration::from_secs(1);
        while metrics.iter().any(|m|m.occupied.load(Ordering::Acquire)) {assert!(Instant::now()<limit);thread::yield_now();}
    }
    // Existing stage bins survive only while the owner threads are alive.
    // Snapshot once after all measured work; preserve full raw bins and max.
    let stages = crate::latency::offline_snapshot_and_reset();
    std::fs::write(format!("{out}-stages.json"), serde_json::to_vec(&stages).unwrap()).unwrap();
    let (mut probe_records,mut probe_dropped)=delivery_probe::take();
    drop(lanes);for h in handles{let(rows,dropped)=h.join().unwrap();probe_records.extend(rows);probe_dropped+=dropped;}
    drop(router_tx);let(router_records,dropped)=router.join().unwrap();probe_records.extend(router_records);probe_dropped+=dropped;
    drop(fast_txs);for h in fast_handles{let(rows,dropped)=h.join().unwrap();probe_records.extend(rows);probe_dropped+=dropped;}
    let (http_records, dropped) = hexagent_runtime::async_rt::block_on_order_runtime(async { delivery_probe::take() });
    probe_records.extend(http_records); probe_dropped += dropped;
    assert_eq!(probe_dropped,0,"incomplete offline evidence");
    if probing {
        let rows: Vec<_> = probe_records.iter().map(|r| (r.owner,r.phase as u8,r.key,r.start,r.end,r.cpu_ns,r.aux,r.status)).collect();
        std::fs::write(format!("{out}-delivery.json"),serde_json::to_vec(&rows).unwrap()).unwrap();
    }
    let completed:u64=metrics.iter().map(|m|m.completed.load(Ordering::Relaxed)).sum();
    let dropped:u64=metrics.iter().map(|m|m.diagnostic_dropped.load(Ordering::Relaxed)).sum();
    let failures:u64=metrics.iter().map(|m|m.send_failures.load(Ordering::Relaxed)).sum();
    assert_eq!(completed,(waves+WARM) as u64*LANES as u64);assert_eq!(replies.load(Ordering::Relaxed),completed);
    assert_eq!(failures,0);assert_eq!(dropped,0);
    finish_execution_diagnostics(metrics,diag_tx,pub_tx,diagnostics);
    shutdown.request();shutdown.finish();shared.join_background_workers();
    stop.store(true,Ordering::Release);server.join().unwrap();
    let mut f=std::io::BufWriter::new(std::fs::File::create(format!("{out}.csv")).unwrap());
    writeln!(f,"wave,slot,input,send_start,send_end,delivered,router_start,router_end,fast_start,fast_end,fast_wait").unwrap();
    for (a,b,c,d,e,f1,g,h,i,j,k) in rows {writeln!(f,"{a},{b},{c},{d},{e},{f1},{g},{h},{i},{j},{k}").unwrap();}
    let mut f=std::io::BufWriter::new(std::fs::File::create(format!("{out}-health.csv")).unwrap());
    writeln!(f,"wave,slot,sequence,observed,consumed,generation,quarantined").unwrap();
    for(a,b,c,d,e,f1,g) in health_rows{writeln!(f,"{a},{b},{c},{d},{e},{f1},{g}").unwrap();}
    std::fs::write(format!("{out}.json"),serde_json::to_vec_pretty(&serde_json::json!({"fast_wait":fast_wait,"reactor_wait":reactor_wait,"completion_wait":completion_wait,"control_wait":control_wait,"private_owner_mode":if legacy_private {"compatibility"} else {"unified"},"completion_receive":"production-recv","router_ingress":if legacy_router {"legacy-crossbeam"} else {"LiveSignalIngress"},"delivery_probe":probing,"probe_dropped":probe_dropped,"completion_backend":completion_backend,"waves":waves,"idle_every":idle_every,"idle_heartbeats":idle_heartbeats,"warmup":WARM,"lanes":LANES,"batch":budget,"cores":cores,"router_us":router_us,"http_delay_us":delay_us,"completed":completed,"http_replies":replies.load(Ordering::Relaxed),"command_capacity":1,"command_hwm":1,"command_overflow":failures,"diagnostic_capacity":EXECUTION_DIAGNOSTIC_CAPACITY,"diagnostic_hwm":diag_hwm,"diagnostic_dropped":dropped,"lifecycle_capacity":LANES,"lifecycle_sampled_hwm":update_hwm,"missed_periods":missed,"formatted_bytes":FORMATTED.load(Ordering::Relaxed),"fixture_version":2,"fast_receive":"production-interruptible-100ms","fast_work":"synthetic 60us thread CPU, six physical owners, three owners per core","transport":"actual loopback TCP HTTP/1.1, no TLS/venue/network RTT"})).unwrap()).unwrap();
}
