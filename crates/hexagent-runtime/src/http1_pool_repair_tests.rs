use super::*;
use std::io::{Read, Write};

/// Fail a status probe, then a body drain, then keep the successful socket
/// alive for its first real request. This exercises admission, not just flags.
#[tokio::test(flavor = "current_thread")]
async fn instrumented_failed_probes_stay_quarantined_until_complete_warm_response() {
    failed_probes_stay_quarantined_until_complete_warm_response(true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn reqwest_failed_probes_stay_quarantined_until_complete_warm_response() {
    failed_probes_stay_quarantined_until_complete_warm_response(false).await;
}
async fn failed_probes_stay_quarantined_until_complete_warm_response(instrumented: bool) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/time", listener.local_addr().unwrap());
    let probes = Arc::new(AtomicUsize::new(0));
    let observed = probes.clone();
    let server = std::thread::spawn(move || {
        for turn in 0..3 {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buf = [0_u8; 4096];
            assert!(socket.read(&mut buf).unwrap() > 0);
            observed.fetch_add(1, Ordering::Release);
            match turn {
                0 => socket.write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap(),
                1 => socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 99\r\nConnection: close\r\n\r\nx").unwrap(),
                _ => {
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK").unwrap();
                    assert!(socket.read(&mut buf).unwrap() > 0);
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK").unwrap();
                }
            }
        }
    });
    let pool = RolePool::new(2, Duration::from_secs(2), Role::Fast).unwrap();
    let permit = pool.try_acquire().unwrap();
    let health = permit.health(permit.generation());
    assert!(health.claim_rebuild(1, Duration::ZERO).is_some());
    drop(permit);
    let healthy_sibling = pool.try_acquire().expect("other slot stays available");
    assert_eq!(healthy_sibling.slot(), 1);
    let stale_health = health.clone();
    let repair_url = url.clone();
    let repair = tokio::spawn(async move {
        if instrumented {
            health.repair_instrumented(repair_url, 1).await;
        } else {
            health.rebuild_and_prewarm(repair_url, 1).await;
        }
    });
    for expected in 1..=2 {
        tokio::time::timeout(Duration::from_secs(3), async {
            while probes.load(Ordering::Acquire) < expected {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            pool.try_acquire().is_none(),
            "failed/incomplete probe must not admit a cold request"
        );
        assert!(
            stale_health.claim_rebuild(1, Duration::ZERO).is_none(),
            "only one retry task per slot"
        );
        assert_eq!(pool.slots[0].generation.load(Ordering::Acquire), 0);
    }
    tokio::time::timeout(Duration::from_secs(4), repair)
        .await
        .unwrap()
        .unwrap();
    let warmed = pool
        .try_acquire()
        .expect("successful body drain returns the slot");
    assert_eq!(warmed.slot(), 0);
    assert_eq!(warmed.generation(), 1);
    if instrumented {
        let response = warmed
            .instrumented
            .request(
                reqwest::Method::GET,
                &url,
                reqwest::header::HeaderMap::new(),
                bytes::Bytes::new(),
                Duration::from_secs(2),
            )
            .await
            .unwrap();
        assert!(
            !response.timings.connect_attempted,
            "business request uses the prewarmed socket"
        );
        assert_eq!(response.body.as_ref(), b"OK");
    } else {
        // Server only accepts three sockets: this request must reuse the third.
        let response = warmed
            .client
            .get(&url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(response.as_ref(), b"OK");
    }
    // A late repair from the retired generation cannot release a new repair.
    let current = warmed.health(warmed.generation());
    assert!(current.claim_rebuild(1, Duration::ZERO).is_some());
    if instrumented {
        stale_health.repair_instrumented(url, 1).await;
    } else {
        stale_health.rebuild_and_prewarm(url, 1).await;
    }
    assert!(pool.slots[0].quarantined.load(Ordering::Acquire));
    drop(warmed);
    assert!(pool.try_acquire().is_none());
    server.join().unwrap();
}
