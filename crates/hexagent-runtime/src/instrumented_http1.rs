//! HTTP/1.1 transport with connection-generation and phase timing.
//!
//! The normal `reqwest` response clock cannot distinguish a reused socket
//! from an implicit reconnect. This connector keeps the pool size unchanged,
//! but puts clocks at the resolver, TCP connector, TLS connector, response
//! headers, and body boundaries. A generation advances only after a new
//! TCP+TLS connection has completed, so callers can prove reuse versus a
//! transparent reconnect without guessing from total latency.

use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper::{Request, StatusCode, Uri};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::dns::{GaiAddrs, GaiResolver, Name};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tower_service::Service;

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

#[derive(Default)]
struct ConnectTrace {
    attempts: AtomicU64,
    generation: AtomicU64,
    reuse_generation_reported: AtomicU64,
    dns_ns: AtomicU64,
    dns_tcp_ns: AtomicU64,
    tls_total_ns: AtomicU64,
}

#[derive(Clone)]
struct TimedResolver {
    inner: GaiResolver,
    trace: Arc<ConnectTrace>,
}

impl Service<Name> for TimedResolver {
    type Response = GaiAddrs;
    type Error = std::io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let mut inner = self.inner.clone();
        let trace = Arc::clone(&self.trace);
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.call(name).await;
            trace
                .dns_ns
                .store(duration_ns(started.elapsed()), Ordering::Release);
            result
        })
    }
}

#[derive(Clone)]
struct TimedTcpConnector {
    inner: HttpConnector<TimedResolver>,
    trace: Arc<ConnectTrace>,
}

impl Service<Uri> for TimedTcpConnector {
    type Response = <HttpConnector<TimedResolver> as Service<Uri>>::Response;
    type Error = <HttpConnector<TimedResolver> as Service<Uri>>::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let mut inner = self.inner.clone();
        let trace = Arc::clone(&self.trace);
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.call(uri).await;
            trace
                .dns_tcp_ns
                .store(duration_ns(started.elapsed()), Ordering::Release);
            result
        })
    }
}

#[derive(Clone)]
struct TimedTlsConnector {
    inner: HttpsConnector<TimedTcpConnector>,
    trace: Arc<ConnectTrace>,
}

impl Service<Uri> for TimedTlsConnector {
    type Response = <HttpsConnector<TimedTcpConnector> as Service<Uri>>::Response;
    type Error = <HttpsConnector<TimedTcpConnector> as Service<Uri>>::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        self.trace.dns_ns.store(0, Ordering::Relaxed);
        self.trace.dns_tcp_ns.store(0, Ordering::Relaxed);
        self.trace.tls_total_ns.store(0, Ordering::Relaxed);
        self.trace.attempts.fetch_add(1, Ordering::AcqRel);
        let mut inner = self.inner.clone();
        let trace = Arc::clone(&self.trace);
        Box::pin(async move {
            let started = Instant::now();
            let result = inner.call(uri).await;
            trace
                .tls_total_ns
                .store(duration_ns(started.elapsed()), Ordering::Release);
            if result.is_ok() {
                trace.generation.fetch_add(1, Ordering::AcqRel);
            }
            result
        })
    }
}

type H1Client = Client<TimedTlsConnector, Full<Bytes>>;

#[derive(Clone)]
pub struct InstrumentedHttp1Client {
    client: H1Client,
    trace: Arc<ConnectTrace>,
    /// One HTTP/1 request per logical slot. Account order lanes already own an
    /// exclusive permit; this also fences exempt Query/heartbeat overlap so
    /// phase attribution remains request-exact and Hyper cannot open a second
    /// active CLOB socket behind the same slot.
    request_gate: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Http1PhaseTimings {
    pub connect_attempted: bool,
    pub connect_generation_before: u64,
    pub connect_generation_after: u64,
    pub dns_ns: u64,
    pub tcp_ns: u64,
    pub tls_ns: u64,
    /// Time from dispatch until response headers, excluding a connect made by
    /// this request. On a reused socket this is the raw header wait.
    pub ttfb_ns: u64,
    pub body_ns: u64,
    pub total_ns: u64,
    pub slot_wait_ns: u64,
    /// True only on the first observed reuse of this slot's current
    /// connection generation. This supports sparse generation logging without
    /// a process-global mutable sampler.
    pub first_reuse_for_generation: bool,
}

impl Http1PhaseTimings {
    pub fn reused_connection(self) -> bool {
        !self.connect_attempted && self.connect_generation_before != 0
    }

    pub fn transparent_reconnect(self) -> bool {
        self.connect_attempted && self.connect_generation_before != 0
    }
}

#[derive(Debug)]
pub struct InstrumentedHttp1Response {
    pub status: StatusCode,
    pub body: Bytes,
    pub timings: Http1PhaseTimings,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentedHttp1ErrorKind {
    Timeout,
    Transport,
    InvalidRequest,
}

#[derive(Debug)]
pub struct InstrumentedHttp1Error {
    pub kind: InstrumentedHttp1ErrorKind,
    pub message: String,
    pub timings: Http1PhaseTimings,
}

impl InstrumentedHttp1Client {
    pub fn new(connect_timeout: Duration) -> anyhow::Result<Self> {
        let trace = Arc::new(ConnectTrace::default());
        let resolver = TimedResolver {
            inner: GaiResolver::new(),
            trace: Arc::clone(&trace),
        };
        let mut http = HttpConnector::new_with_resolver(resolver);
        http.enforce_http(false);
        http.set_connect_timeout(Some(connect_timeout));
        http.set_keepalive(Some(Duration::from_secs(30)));
        http.set_nodelay(true);
        let tcp = TimedTcpConnector {
            inner: http,
            trace: Arc::clone(&trace),
        };
        let https = HttpsConnectorBuilder::new()
            // CLOB is a public Internet endpoint. The compiled WebPKI store
            // avoids per-slot platform trust-store I/O during parallel pool
            // construction while retaining ordinary public CA validation.
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .wrap_connector(tcp);
        let connector = TimedTlsConnector {
            inner: https,
            trace: Arc::clone(&trace),
        };
        let mut builder = Client::builder(TokioExecutor::new());
        // Never let the generic client replay an order internally. A stale
        // pooled socket returns Transport and enters the external place gate;
        // only the explicitly idempotent cancel path may hedge.
        builder.retry_canceled_requests(false);
        builder.pool_idle_timeout(Duration::from_secs(300));
        // The per-slot request gate makes a second active connection
        // unnecessary; retain exactly one idle CLOB socket per logical slot.
        builder.pool_max_idle_per_host(1);
        let client = builder.build(connector);
        Ok(Self {
            client,
            trace,
            request_gate: Arc::new(tokio::sync::Semaphore::new(1)),
        })
    }

    pub async fn request(
        &self,
        method: reqwest::Method,
        url: &str,
        headers: reqwest::header::HeaderMap,
        body: Bytes,
        timeout: Duration,
    ) -> Result<InstrumentedHttp1Response, InstrumentedHttp1Error> {
        let gate_started = Instant::now();
        let _request_guard = self
            .request_gate
            .acquire()
            .await
            .expect("instrumented HTTP/1 request gate is never closed");
        let slot_wait_ns = duration_ns(gate_started.elapsed());
        let attempts_before = self.trace.attempts.load(Ordering::Acquire);
        let generation_before = self.trace.generation.load(Ordering::Acquire);
        let request = match Request::builder()
            .method(method)
            .uri(url)
            .body(Full::new(body))
        {
            Ok(mut request) => {
                *request.headers_mut() = headers;
                request
            }
            Err(error) => {
                return Err(InstrumentedHttp1Error {
                    kind: InstrumentedHttp1ErrorKind::InvalidRequest,
                    message: error.to_string(),
                    timings: Http1PhaseTimings::default(),
                })
            }
        };
        let started = Instant::now();
        let operation = async {
            let headers_started = Instant::now();
            let response = self
                .client
                .request(request)
                .await
                .map_err(|error| (InstrumentedHttp1ErrorKind::Transport, error.to_string()))?;
            let headers_ns = duration_ns(headers_started.elapsed());
            let status = response.status();
            let body_started = Instant::now();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|error| (InstrumentedHttp1ErrorKind::Transport, error.to_string()))?
                .to_bytes();
            let body_ns = duration_ns(body_started.elapsed());
            Ok::<_, (InstrumentedHttp1ErrorKind, String)>((status, body, headers_ns, body_ns))
        };
        match tokio::time::timeout(timeout, operation).await {
            Ok(Ok((status, body, headers_ns, body_ns))) => {
                let timings = self.snapshot(
                    attempts_before,
                    generation_before,
                    headers_ns,
                    body_ns,
                    duration_ns(started.elapsed()),
                    slot_wait_ns,
                );
                Ok(InstrumentedHttp1Response {
                    status,
                    body,
                    timings,
                })
            }
            Ok(Err((kind, message))) => Err(InstrumentedHttp1Error {
                kind,
                message,
                timings: self.snapshot(
                    attempts_before,
                    generation_before,
                    duration_ns(started.elapsed()),
                    0,
                    duration_ns(started.elapsed()),
                    slot_wait_ns,
                ),
            }),
            Err(_) => Err(InstrumentedHttp1Error {
                kind: InstrumentedHttp1ErrorKind::Timeout,
                message: format!("HTTP/1.1 request timed out after {}ms", timeout.as_millis()),
                timings: self.snapshot(
                    attempts_before,
                    generation_before,
                    duration_ns(started.elapsed()),
                    0,
                    duration_ns(started.elapsed()),
                    slot_wait_ns,
                ),
            }),
        }
    }

    fn snapshot(
        &self,
        attempts_before: u64,
        generation_before: u64,
        headers_ns: u64,
        body_ns: u64,
        total_ns: u64,
        slot_wait_ns: u64,
    ) -> Http1PhaseTimings {
        let attempts_after = self.trace.attempts.load(Ordering::Acquire);
        let generation_after = self.trace.generation.load(Ordering::Acquire);
        let connect_attempted = attempts_after != attempts_before;
        let first_reuse_for_generation = if !connect_attempted && generation_after != 0 {
            self.trace
                .reuse_generation_reported
                .swap(generation_after, Ordering::AcqRel)
                != generation_after
        } else {
            false
        };
        let (dns_ns, tcp_ns, tls_ns) = if connect_attempted {
            let dns_ns = self.trace.dns_ns.load(Ordering::Acquire);
            let dns_tcp_ns = self.trace.dns_tcp_ns.load(Ordering::Acquire);
            let tls_total_ns = self.trace.tls_total_ns.load(Ordering::Acquire);
            (
                dns_ns,
                dns_tcp_ns.saturating_sub(dns_ns),
                tls_total_ns.saturating_sub(dns_tcp_ns),
            )
        } else {
            (0, 0, 0)
        };
        Http1PhaseTimings {
            connect_attempted,
            connect_generation_before: generation_before,
            connect_generation_after: generation_after,
            dns_ns,
            tcp_ns,
            tls_ns,
            ttfb_ns: headers_ns
                .saturating_sub(dns_ns)
                .saturating_sub(tcp_ns)
                .saturating_sub(tls_ns),
            body_ns,
            total_ns,
            slot_wait_ns,
            first_reuse_for_generation,
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    async fn serve_one_request(
        listener: &tokio::net::TcpListener,
        close: bool,
    ) -> tokio::net::TcpStream {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0_u8; 2048];
        let _ = stream.read(&mut request).await.unwrap();
        let connection = if close { "close" } else { "keep-alive" };
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: {connection}\r\n\r\nok"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        stream.flush().await.unwrap();
        stream
    }

    #[tokio::test(flavor = "current_thread")]
    async fn generation_distinguishes_reuse_from_transparent_reconnect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut first = serve_one_request(&listener, false).await;
            let mut request = [0_u8; 2048];
            let _ = first.read(&mut request).await.unwrap();
            first
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
            first.flush().await.unwrap();
            drop(first);
            let _second = serve_one_request(&listener, true).await;
        });
        let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        let url = format!("http://{addr}/time");
        let request = || {
            client.request(
                reqwest::Method::GET,
                &url,
                reqwest::header::HeaderMap::new(),
                Bytes::new(),
                Duration::from_secs(1),
            )
        };
        let initial = request().await.unwrap();
        assert!(initial.timings.connect_attempted);
        assert!(!initial.timings.first_reuse_for_generation);
        assert_eq!(initial.timings.connect_generation_before, 0);
        assert_eq!(initial.timings.connect_generation_after, 1);

        let reused = request().await.unwrap();
        assert!(reused.timings.reused_connection());
        assert!(reused.timings.first_reuse_for_generation);
        assert_eq!(reused.timings.connect_generation_before, 1);
        assert_eq!(reused.timings.connect_generation_after, 1);

        let reconnected = request().await.unwrap();
        assert!(reconnected.timings.transparent_reconnect());
        assert!(!reconnected.timings.first_reuse_for_generation);
        assert_eq!(reconnected.timings.connect_generation_before, 1);
        assert_eq!(reconnected.timings.connect_generation_after, 2);
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_callers_serialize_on_one_slot_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
                )
                .await
                .unwrap();
            stream.flush().await.unwrap();
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .unwrap();
            stream.flush().await.unwrap();
        });
        let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        let url = format!("http://{addr}/time");
        let request = || {
            client.request(
                reqwest::Method::GET,
                &url,
                reqwest::header::HeaderMap::new(),
                Bytes::new(),
                Duration::from_secs(1),
            )
        };
        let (first, second) = tokio::join!(request(), request());
        let first = first.unwrap().timings;
        let second = second.unwrap().timings;
        assert_eq!(
            u8::from(first.connect_attempted) + u8::from(second.connect_attempted),
            1,
        );
        assert_eq!(first.connect_generation_after, 1);
        assert_eq!(second.connect_generation_after, 1);
        assert!(first.slot_wait_ns.max(second.slot_wait_ns) >= 20_000_000);
        server.await.unwrap();
    }
}
