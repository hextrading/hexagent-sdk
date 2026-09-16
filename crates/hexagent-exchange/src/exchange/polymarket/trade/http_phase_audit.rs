//! Fixed-size HTTP observations. The I/O runtime only try-sends; the existing
//! account audit worker owns formatting, histograms and batched file writes.
//! This shares the 4096-entry advisory audit FIFO, never the lossless private
//! lifecycle lane. Overflow drops only telemetry and increments a counter.
use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) struct HttpPhaseContext {
    pub root_attempt_id: u64,
    pub leg: u8,
    pub kind: &'static str,
    pub runtime_queue_ns: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct HttpPhaseRecord {
    pub context: HttpPhaseContext,
    pub attempt_id: u64,
    pub role: crate::http1_pool::Role,
    pub slot: usize,
    pub request_started_ns: u64,
    pub response_received_ns: u64,
    pub completed_ns: u64,
    pub status: u16,
    pub outcome: &'static str,
    pub timings: crate::instrumented_http1::Http1PhaseTimings,
}

pub(super) struct HttpPhaseAudit {
    tx: crossbeam_channel::Sender<AuditJob>,
    dropped: AtomicU64,
    high_water: AtomicU64,
}

impl HttpPhaseAudit {
    pub fn new(tx: crossbeam_channel::Sender<AuditJob>) -> Self {
        Self {
            tx,
            dropped: AtomicU64::new(0),
            high_water: AtomicU64::new(0),
        }
    }

    pub fn publish(&self, record: HttpPhaseRecord) {
        // Advisory shared-lane occupancy, sampled immediately before enqueue.
        // No allocation, string formatting, histogram update or retry here.
        self.high_water.fetch_max(
            self.tx
                .len()
                .saturating_add(1)
                .min(EXECUTION_AUDIT_QUEUE_CAPACITY) as u64,
            Ordering::Relaxed,
        );
        if self.tx.try_send(AuditJob::HttpPhase(record)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn write_metrics(
        &self,
        recorder: &mut OrderAttemptRecorder,
        account: &str,
    ) -> std::io::Result<()> {
        if self.high_water.load(Ordering::Relaxed) == 0 {
            return Ok(());
        }
        recorder.write_record(&serde_json::json!({
            "kind": "http_phase_audit_metric", "account": account,
            "timestamp_ns": now_ns(), "queue_capacity": EXECUTION_AUDIT_QUEUE_CAPACITY,
            "queue_depth": self.tx.len(),
            "queue_high_water": self.high_water.load(Ordering::Relaxed),
            "phase_dropped": self.dropped.load(Ordering::Relaxed),
        }))
    }
}

impl HttpPhaseRecord {
    pub fn value(&self, account: &str) -> serde_json::Value {
        let t = self.timings;
        serde_json::json!({
            "kind": "http_phase", "account": account,
            "attempt_id": self.attempt_id, "root_attempt_id": self.context.root_attempt_id,
            "leg": self.context.leg, "request_kind": self.context.kind,
            "role": format!("{:?}", self.role), "slot": self.slot,
            "request_started_ns": self.request_started_ns,
            "response_received_ns": self.response_received_ns,
            "completed_ns": self.completed_ns, "status_code": self.status, "outcome": self.outcome,
            "runtime_queue_ns": self.context.runtime_queue_ns,
            "response_processing_ns": self.completed_ns.saturating_sub(self.response_received_ns),
            "slot_wait_ns": t.slot_wait_ns, "dns_ns": t.dns_ns, "tcp_ns": t.tcp_ns,
            "tls_ns": t.tls_ns, "ttfb_ns": t.ttfb_ns, "body_ns": t.body_ns, "total_ns": t.total_ns,
            "connect_attempted": t.connect_attempted,
            "generation_before": t.connect_generation_before, "generation_after": t.connect_generation_after,
            "peer": t.peer.map(|peer| peer.to_string()), "incomplete_phase": t.incomplete_phase.name(),
        })
    }
}

pub(super) fn request_kind(
    kind: Option<crate::latency_record::RequestKind>,
    fallback: &'static str,
) -> &'static str {
    use crate::latency_record::RequestKind::*;
    match kind {
        Some(Place) => "place",
        Some(Cancel) => "cancel",
        Some(ProbePlace) => "probe_place",
        Some(ProbeCancel) => "probe_cancel",
        None => fallback,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(id: u64, root: u64, leg: u8) -> HttpPhaseRecord {
        HttpPhaseRecord {
            context: HttpPhaseContext {
                root_attempt_id: root,
                leg,
                kind: "cancel",
                runtime_queue_ns: 7,
            },
            attempt_id: id,
            role: crate::http1_pool::Role::Cancel,
            slot: leg as usize,
            request_started_ns: 10,
            response_received_ns: 20,
            completed_ns: 25,
            status: 200,
            outcome: "ok",
            timings: crate::instrumented_http1::Http1PhaseTimings {
                slot_wait_ns: 3,
                total_ns: 8,
                ..Default::default()
            },
        }
    }
    #[test]
    fn bounded_phase_lane_preserves_root_hedge_order_and_counts_loss() {
        let (tx, rx) = crossbeam_channel::bounded(2);
        let sink = HttpPhaseAudit::new(tx);
        sink.publish(record(10, 10, 0));
        sink.publish(record(11, 10, 1));
        sink.publish(record(12, 12, 0));
        assert_eq!(sink.dropped.load(Ordering::Relaxed), 1);
        for (id, leg) in [(10, 0), (11, 1)] {
            let AuditJob::HttpPhase(r) = rx.try_recv().unwrap() else {
                panic!()
            };
            assert_eq!(
                (r.attempt_id, r.context.root_attempt_id, r.context.leg),
                (id, 10, leg)
            );
            let v = r.value("account-a");
            assert_eq!(v["account"], "account-a");
            assert_eq!(v["slot_wait_ns"], 3);
            assert_eq!(v["response_processing_ns"], 5);
        }
        drop(rx);
        sink.publish(record(13, 13, 0));
        assert_eq!(sink.dropped.load(Ordering::Relaxed), 2);
    }
    #[test]
    fn different_accounts_have_independent_telemetry_and_probe_classification() {
        let (a, ar) = crossbeam_channel::bounded(1);
        let (b, br) = crossbeam_channel::bounded(1);
        let a = HttpPhaseAudit::new(a);
        let b = HttpPhaseAudit::new(b);
        a.publish(record(1, 1, 0));
        a.publish(record(2, 2, 0));
        b.publish(record(3, 3, 0));
        assert_eq!(a.dropped.load(Ordering::Relaxed), 1);
        assert_eq!(b.dropped.load(Ordering::Relaxed), 0);
        assert_eq!(ar.len(), 1);
        assert_eq!(br.len(), 1);
        assert_eq!(
            request_kind(
                Some(crate::latency_record::RequestKind::ProbePlace),
                "query"
            ),
            "probe_place"
        );
        assert_eq!(request_kind(None, "query"), "query");
        assert!(std::mem::size_of::<HttpPhaseRecord>() <= 256);
    }
}

#[cfg(test)]
#[test]
#[ignore = "focused I/O-owner telemetry handoff benchmark; serial only"]
fn benchmark_http_phase_publish() {
    let (tx, rx) = crossbeam_channel::bounded(EXECUTION_AUDIT_QUEUE_CAPACITY);
    let sink = Arc::new(HttpPhaseAudit::new(tx));
    let timings = crate::instrumented_http1::Http1PhaseTimings {
        slot_wait_ns: 10,
        ttfb_ns: 60_000_000,
        body_ns: 100,
        total_ns: 60_000_100,
        ..Default::default()
    };
    const N: usize = 100_000;
    let mut ns = [Vec::with_capacity(N), Vec::with_capacity(N)];
    for i in 0..N + 512 {
        for v in [i % 2, 1 - i % 2] {
            let start = std::time::Instant::now();
            if v == 0 {
                record_http1_phase_timings(crate::http1_pool::Role::Fast, 0, timings);
            } else {
                let cloned = Arc::clone(&sink);
                cloned.publish(HttpPhaseRecord {
                    context: HttpPhaseContext {
                        root_attempt_id: i as u64,
                        leg: 0,
                        kind: "place",
                        runtime_queue_ns: 0,
                    },
                    attempt_id: i as u64,
                    role: crate::http1_pool::Role::Fast,
                    slot: 0,
                    request_started_ns: 1,
                    response_received_ns: 2,
                    completed_ns: 3,
                    status: 200,
                    outcome: "ok",
                    timings,
                });
            }
            let elapsed = start.elapsed().as_nanos() as u64;
            if v == 1 {
                std::hint::black_box(rx.try_recv().unwrap());
            }
            if i >= 512 {
                ns[v].push(elapsed);
            }
        }
    }
    for v in 0..2 {
        ns[v].sort_unstable();
        eprintln!("http_phase_publish version={v} n={N} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_hwm={} phase_dropped={} boundary=old_inline_histograms_or_new_arc_clone_try_send worker_dequeue_and_export_excluded=true",ns[v][N/2],ns[v][N*99/100],ns[v][N*999/1000],ns[v][N-1],sink.high_water.load(Ordering::Relaxed),sink.dropped.load(Ordering::Relaxed));
    }
}

#[cfg(test)]
#[tokio::test]
async fn actual_http_phases_keep_identity_status_peer_and_reconnect_generation() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    crate::async_rt::init().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for (status, body) in [
            ("200 OK", "{}"),
            ("503 Service Unavailable", "{}"),
            ("200 OK", "not-json"),
        ] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut input = vec![0u8; 8192];
            let mut used = 0;
            loop {
                let n = stream.read(&mut input[used..]).await.unwrap();
                assert!(n > 0);
                used += n;
                if input[..used].windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let (tx, rx) = crossbeam_channel::bounded(4);
    let sink = HttpPhaseAudit::new(tx);
    let client = crate::http1_pool::pooled_client(crate::http1_pool::Role::Query);
    let auth = super::super::auth::AuthHeaders {
        api_key: "test".into(),
        address: "test".into(),
        signature: "test".into(),
        timestamp: "1".into(),
        passphrase: "test".into(),
    };
    let url: Arc<str> = format!("http://{peer}/test").into();
    let mut generation = 0;
    for (index, expected) in ["ok", "http_error", "invalid_response"]
        .into_iter()
        .enumerate()
    {
        let attempt_id = client.allocate_attempt_id();
        let reply = execute_http_on(
            client.clone(),
            attempt_id,
            &reqwest::Method::GET,
            &url,
            "/test",
            &auth,
            Bytes::new(),
            &sink,
            HttpPhaseContext {
                root_attempt_id: attempt_id,
                leg: 0,
                kind: "query",
                runtime_queue_ns: 13,
            },
        )
        .await;
        assert_eq!(reply.is_ok(), index == 0);
        let AuditJob::HttpPhase(r) = rx.try_recv().unwrap() else {
            panic!()
        };
        assert_eq!(r.attempt_id, attempt_id);
        assert_eq!(r.context.root_attempt_id, attempt_id);
        assert_eq!(r.outcome, expected);
        assert_eq!(r.timings.peer, Some(peer));
        assert!(r.timings.connect_generation_after > generation);
        generation = r.timings.connect_generation_after;
        assert!(
            r.request_started_ns <= r.response_received_ns
                && r.response_received_ns <= r.completed_ns
        );
        assert_eq!(r.context.runtime_queue_ns, 13);
    }
    server.await.unwrap();
    assert!(rx.is_empty());
}
