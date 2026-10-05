use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn read_headers(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut buf = [0; 1024];
    while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
    }
    bytes
}

async fn preempt_stalled_probe() -> Http1PhaseTimings {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
    let warm_client = client.clone();
    let warm_url = url.clone();
    let warm = tokio::spawn(async move {
        warm_client
            .keep_warm(&warm_url, Duration::from_secs(5))
            .await
    });
    let (mut stalled, _) = listener.accept().await.unwrap();
    assert!(read_headers(&mut stalled).await.starts_with(b"GET /"));
    assert!(matches!(
        client.keep_warm(&url, Duration::from_secs(1)).await,
        KeepWarmOutcome::Busy
    ));
    let business = tokio::spawn(async move {
        client
            .request(
                reqwest::Method::DELETE,
                &url,
                reqwest::header::HeaderMap::new(),
                Bytes::new(),
                Duration::from_secs(1),
            )
            .await
    });
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(200), warm)
            .await
            .unwrap()
            .unwrap(),
        KeepWarmOutcome::Preempted
    ));
    let (mut replacement, _) = tokio::time::timeout(Duration::from_millis(200), listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert!(read_headers(&mut replacement)
        .await
        .starts_with(b"DELETE /"));
    replacement
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\ncancel")
        .await
        .unwrap();
    let response = business.await.unwrap().unwrap();
    assert_eq!(response.body, Bytes::from_static(b"cancel"));
    assert!(response.timings.slot_wait_ns < 200_000_000);
    assert!(
        response.timings.connect_attempted,
        "preemption cold cost must remain measurable"
    );
    let mut byte = [0; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(200), stalled.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0,
        "the interrupted GET socket must close rather than linger alongside business"
    );
    response.timings
}

#[tokio::test(flavor = "current_thread")]
async fn stalled_keep_warm_is_preempted_by_business_without_waiting_for_deadline() {
    preempt_stalled_probe().await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release loopback benchmark: interrupted maintenance including measured cold reconnect"]
async fn benchmark_keep_warm_preemption() {
    let mut queue = Vec::with_capacity(1000);
    let mut total = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let t = preempt_stalled_probe().await;
        queue.push(t.slot_wait_ns);
        total.push(t.slot_wait_ns + t.total_ns);
    }
    for (boundary, mut v) in [
        ("business_slot_wait", queue),
        ("business_gate_through_cold_response", total),
    ] {
        v.sort_unstable();
        println!("keep_warm_preemption boundary={boundary} n=1000 p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water=1 overflow=0 maintenance_deadline_ms=5000 cold_reconnects=1000",
            v[499], v[989], v[998], v[999]);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn business_queue_and_duplicate_maintenance_have_priority_without_trace_corruption() {
    let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
    let guard = client.request_gate.acquire().await.unwrap();
    client.trace.generation.store(7, Ordering::Release);
    let business = client.request(
        reqwest::Method::DELETE,
        "http://127.0.0.1:1/",
        reqwest::header::HeaderMap::new(),
        Bytes::new(),
        Duration::from_millis(20),
    );
    tokio::pin!(business);
    tokio::select! { biased; _ = &mut business => panic!("must be queued"), _ = tokio::task::yield_now() => {} }
    assert!(matches!(
        client
            .keep_warm("http://127.0.0.1:1/", Duration::from_secs(1))
            .await,
        KeepWarmOutcome::Busy
    ));
    assert_eq!(client.trace.generation.load(Ordering::Acquire), 7);
    assert_eq!(client.trace.attempts.load(Ordering::Acquire), 0);
    assert_eq!(
        business.await.unwrap_err().kind,
        InstrumentedHttp1ErrorKind::QueueTimeout
    );
    assert_eq!(client.maintenance.business.load(Ordering::SeqCst), 0);
    assert!(!client.maintenance.active.load(Ordering::SeqCst));
    drop(guard);
}

#[tokio::test(flavor = "current_thread")]
async fn completed_keep_warm_reuses_socket_and_does_not_cancel_next_probe() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for _ in 0..4 {
            read_headers(&mut stream).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await
                .unwrap();
        }
        stream
    });
    let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
    for i in 0..4 {
        let response = if i % 2 == 0 {
            match client.keep_warm(&url, Duration::from_secs(1)).await {
                KeepWarmOutcome::Completed(Ok(response)) => response,
                _ => panic!("idle maintenance must complete"),
            }
        } else {
            client
                .request(
                    reqwest::Method::DELETE,
                    &url,
                    reqwest::header::HeaderMap::new(),
                    Bytes::new(),
                    Duration::from_secs(1),
                )
                .await
                .unwrap()
        };
        assert_eq!(response.timings.connect_attempted, i == 0);
        assert_eq!(response.timings.connect_generation_after, 1);
    }
    drop(server.await.unwrap());
}
