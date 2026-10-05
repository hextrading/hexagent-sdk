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
use hyper_util::client::legacy::connect::dns::{GaiResolver, Name};
use hyper_util::client::legacy::connect::{HttpConnector, HttpInfo};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, AtomicU16, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tower_service::Service;

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

/// Connection establishment is cold work, including synchronous certificate
/// verification inside a TLS future poll. The existing order runtime's
/// demoted blocking workers poll it with that same runtime handle: TCP socket
/// registration remains on the order reactor, while crypto does not delay
/// another slot's first request poll. No worker is added to the quote lane.
/// At most one connect per exclusive physical slot is submitted. The reply
/// is capacity one; dropping the caller cancels the owned connect future,
/// including a pending socket, rather than detaching an unlimited repair.
async fn cold_connect<F>(future: F) -> std::io::Result<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    cold_connect_via(future, crate::cold_connect_submit::try_submit).await
}

pub(crate) async fn cold_connect_via<F>(future: F,
    submit: impl FnOnce(crate::cold_connect_submit::Job) -> std::io::Result<()>,
) -> std::io::Result<F::Output>
where F: Future + Send + 'static, F::Output: Send + 'static,
{
    let runtime = tokio::runtime::Handle::current();
    let (mut tx, rx) = tokio::sync::oneshot::channel();
    submit(Box::new(move || {
        if tx.is_closed() { return; }
        // This call may create an OS worker synchronously. Its caller is the
        // prestarted SCHED_OTHER submit owner, never the order I/O reactor.
        let worker_runtime = runtime.clone();
        runtime.spawn_blocking(move || {
            crate::latency::prepare_scheduler_tail_queue();
            worker_runtime.block_on(async move {
                tokio::select! {
                    biased;
                    _ = tx.closed() => {}
                    result = observe_cold_connect(future) => { let _ = tx.send(result); }
                }
            });
        });
    }))?;
    rx.await.map_err(std::io::Error::other)
}

async fn observe_cold_connect<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(|cx| {
        let started = Instant::now();
        let cpu = crate::latency::thread_cpu_ns();
        let result = future.as_mut().poll(cx);
        let elapsed = duration_ns(started.elapsed());
        if elapsed >= 100_000 {
            let current_cpu = crate::latency::thread_cpu_ns();
            crate::latency::observe_scheduler_tail(crate::latency::SchedulerTail {
                probe: "order_cold_connect",
                observed_unix_ns: hexagent_types::types::now_ns(),
                lag_ns: elapsed,
                span_wall_ns: elapsed,
                span_cpu_ns: (cpu != 0 && current_cpu >= cpu).then(|| current_cpu - cpu),
                expirations: 0,
                error_code: None,
                boundary: "single_connect_future_poll_on_background_worker",
            });
        }
        result
    }).await
}

mod io_trace;
pub use io_trace::Http1IoTimings;
use io_trace::{IoTrace, TimedIo};

#[derive(Default)]
struct ConnectTrace {
    io: IoTrace,
    attempts: AtomicU64,
    generation: AtomicU64,
    closed_generation: AtomicU64,
    reuse_generation_reported: AtomicU64,
    dns_ns: AtomicU64,
    dns_tcp_ns: AtomicU64,
    tls_total_ns: AtomicU64,
    connect_worker_queue_ns: AtomicU64,
    phase: AtomicU8,
    peer_family: AtomicU8,
    peer_port: AtomicU16,
    peer_words: [AtomicU32; 4],
}

const PHASE_DNS: u8 = 1;
const PHASE_TCP: u8 = 2;
const PHASE_TLS: u8 = 3;
const PHASE_TTFB: u8 = 4;
const PHASE_BODY: u8 = 5;

impl ConnectTrace {
    fn store_peer(&self, peer: SocketAddr) {
        self.peer_port.store(peer.port(), Ordering::Release);
        match peer.ip() {
            IpAddr::V4(ip) => {
                self.peer_words[0].store(u32::from(ip), Ordering::Relaxed);
                self.peer_family.store(4, Ordering::Release);
            }
            IpAddr::V6(ip) => {
                let octets = ip.octets();
                for (word, bytes) in self.peer_words.iter().zip(octets.chunks_exact(4)) {
                    word.store(
                        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                        Ordering::Relaxed,
                    );
                }
                self.peer_family.store(6, Ordering::Release);
            }
        }
    }

    fn peer(&self) -> Option<SocketAddr> {
        let port = self.peer_port.load(Ordering::Acquire);
        match self.peer_family.load(Ordering::Acquire) {
            4 => Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(self.peer_words[0].load(Ordering::Acquire))),
                port,
            )),
            6 => {
                let mut octets = [0_u8; 16];
                for (chunk, word) in octets.chunks_exact_mut(4).zip(self.peer_words.iter()) {
                    chunk.copy_from_slice(&word.load(Ordering::Acquire).to_be_bytes());
                }
                Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
            }
            _ => None,
        }
    }
}

#[derive(Clone)]
struct TimedResolver {
    inner: GaiResolver,
    trace: Arc<ConnectTrace>,
    avoid_peer: Option<IpAddr>,
    #[cfg(test)]
    fixed_answers: Option<Vec<SocketAddr>>,
}

// Connection construction only, outside steady-state request dispatch. Use
// current DNS answers; never pin a venue IP or change Host/SNI/TLS validation.
// When DNS offers an alternative, do not fall back to the known failed peer
// during this repair. A sole address remains retryable under repair backoff.
fn repair_addresses(
    addresses: impl Iterator<Item = SocketAddr>,
    avoid_peer: Option<IpAddr>,
) -> std::vec::IntoIter<SocketAddr> {
    let mut addresses: Vec<_> = addresses.collect();
    if let Some(avoid) = avoid_peer {
        if addresses.iter().any(|address| address.ip() != avoid) {
            addresses.retain(|address| address.ip() != avoid);
        }
    }
    addresses.into_iter()
}

impl Service<Name> for TimedResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = std::io::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        #[cfg(test)]
        if let Some(answers) = self.fixed_answers.clone() {
            let avoid = self.avoid_peer;
            return Box::pin(async move { Ok(repair_addresses(answers.into_iter(), avoid)) });
        }
        let mut inner = self.inner.clone();
        let trace = Arc::clone(&self.trace);
        let avoid_peer = self.avoid_peer;
        Box::pin(async move {
            trace.phase.store(PHASE_DNS, Ordering::Release);
            let started = Instant::now();
            let result = inner.call(name).await;
            trace
                .dns_ns
                .store(duration_ns(started.elapsed()), Ordering::Release);
            trace.phase.store(PHASE_TCP, Ordering::Release);
            result.map(|addresses| repair_addresses(addresses, avoid_peer))
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
            trace.phase.store(PHASE_TCP, Ordering::Release);
            let started = Instant::now();
            let result = inner.call(uri).await;
            trace
                .dns_tcp_ns
                .store(duration_ns(started.elapsed()), Ordering::Release);
            if let Ok(stream) = &result {
                if let Ok(peer) = stream.inner().peer_addr() {
                    trace.store_peer(peer);
                }
                trace.phase.store(PHASE_TLS, Ordering::Release);
            }
            result
        })
    }
}

#[derive(Clone)]
struct TimedTlsConnector {
    inner: HttpsConnector<TimedTcpConnector>,
    trace: Arc<ConnectTrace>,
    connect_timeout: Duration,
}

impl Service<Uri> for TimedTlsConnector {
    type Response = TimedIo<<HttpsConnector<TimedTcpConnector> as Service<Uri>>::Response>;
    type Error = <HttpsConnector<TimedTcpConnector> as Service<Uri>>::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        self.trace.dns_ns.store(0, Ordering::Relaxed);
        self.trace.dns_tcp_ns.store(0, Ordering::Relaxed);
        self.trace.tls_total_ns.store(0, Ordering::Relaxed);
        self.trace.connect_worker_queue_ns.store(0, Ordering::Relaxed);
        self.trace.peer_family.store(0, Ordering::Relaxed);
        self.trace.phase.store(PHASE_TLS, Ordering::Release);
        self.trace.attempts.fetch_add(1, Ordering::AcqRel);
        let mut inner = self.inner.clone();
        let trace = Arc::clone(&self.trace);
        let connect_timeout = self.connect_timeout;
        Box::pin(async move {
            let started = Instant::now();
            let deadline = tokio::time::Instant::now() + connect_timeout;
            let worker_trace = Arc::clone(&trace);
            let connect = inner.call(uri);
            let result = tokio::time::timeout_at(deadline, cold_connect(async move {
                worker_trace.connect_worker_queue_ns.store(duration_ns(started.elapsed()), Ordering::Release);
                // Hyper may keep a connect task after an HTTP timeout. Bound
                // the entire cold connect, including worker pickup and TLS,
                // so a silent handshake cannot retain a worker indefinitely.
                tokio::time::timeout_at(deadline, connect).await
                    .map_err(|error| -> <HttpsConnector<TimedTcpConnector> as Service<Uri>>::Error {
                        std::io::Error::new(std::io::ErrorKind::TimedOut, error).into()
                    })?
            })).await
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::TimedOut, error))??;
            trace
                .tls_total_ns
                .store(duration_ns(started.elapsed()), Ordering::Release);
            result.map(|inner| {
                let generation = trace.generation.fetch_add(1, Ordering::AcqRel) + 1;
                trace.phase.store(PHASE_TTFB, Ordering::Release);
                #[cfg(target_os = "linux")]
                let socket_fd = {
                    use std::os::fd::AsRawFd;
                    match &inner {
                        hyper_rustls::MaybeHttpsStream::Http(io) => io.inner().as_raw_fd(),
                        hyper_rustls::MaybeHttpsStream::Https(io) => io.inner().get_ref().0.inner().inner().as_raw_fd(),
                    }
                };
                TimedIo { inner, trace, generation, flush_pending: false,
                    #[cfg(target_os = "linux")]
                    socket_fd,
                }
            })
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
    maintenance: Arc<MaintenancePriority>,
}

#[derive(Default)]
struct MaintenancePriority {
    business: AtomicUsize,
    active: AtomicBool,
    cancel: tokio::sync::Notify,
}

struct BusinessRequest<'a>(&'a MaintenancePriority);
impl Drop for BusinessRequest<'_> {
    fn drop(&mut self) { self.0.business.fetch_sub(1, Ordering::SeqCst); }
}
struct MaintenanceRequest<'a>(&'a MaintenancePriority);
impl Drop for MaintenanceRequest<'_> {
    fn drop(&mut self) { self.0.active.store(false, Ordering::SeqCst); }
}

/// Maintenance never queues behind business and yields its request gate as
/// soon as a business request arrives. A cancelled HTTP/1 GET can retire its
/// socket; the business request then follows the normal measured cold path.
/// It must never wait for the maintenance response deadline.
pub(crate) enum KeepWarmOutcome {
    Busy,
    Preempted,
    Completed(Result<InstrumentedHttp1Response, InstrumentedHttp1Error>),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Http1PhaseTimings {
    pub io: Http1IoTimings,
    pub connect_attempted: bool,
    pub connect_generation_before: u64,
    pub connect_generation_after: u64,
    pub dns_ns: u64,
    pub tcp_ns: u64,
    pub tls_ns: u64,
    /// Cold worker pickup, excluded from TLS and TTFB; zero on socket reuse.
    pub connect_worker_queue_ns: u64,
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
    /// Peer of the reused or newly connected socket. Available before headers
    /// as soon as TCP completes, including TTFB/body timeouts.
    pub peer: Option<SocketAddr>,
    /// Stage that had not completed when the request failed or timed out.
    pub incomplete_phase: Http1IncompletePhase,
}

/// Lock-free identity of the socket currently resident in one logical HTTP/1
/// slot. This is intentionally a small value snapshot so connection owners
/// can attribute a slow request without borrowing the connector or changing
/// pool cardinality.
#[derive(Clone, Copy, Debug, Default)]
pub struct Http1ConnectionSnapshot {
    pub connect_generation: u64,
    pub peer: Option<SocketAddr>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Http1IncompletePhase {
    #[default]
    None,
    Dns,
    Tcp,
    Tls,
    SlotWait,
    Ttfb,
    Body,
}

impl Http1IncompletePhase {
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Dns => "dns",
            Self::Tcp => "tcp",
            Self::Tls => "tls",
            Self::SlotWait => "slot_wait",
            Self::Ttfb => "ttfb",
            Self::Body => "body",
        }
    }
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
    /// Connector failed before HTTP dispatch; the signed order was not sent.
    Connect,
    /// Deadline elapsed while another request still owned this slot.
    QueueTimeout,
    InvalidRequest,
}

#[derive(Debug)]
pub struct InstrumentedHttp1Error {
    pub kind: InstrumentedHttp1ErrorKind,
    pub message: String,
    pub timings: Http1PhaseTimings,
}

// Cold error path only. Display on hyper-util drops the cause (e.g. reset,
// broken pipe, EOF); retain it without printing the request or auth headers.
fn http_error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        use std::fmt::Write as _;
        let _ = write!(message, ": {cause}");
        source = cause.source();
    }
    message
}

impl InstrumentedHttp1Client {
    pub fn new(connect_timeout: Duration) -> anyhow::Result<Self> {
        Self::new_avoiding_peer(connect_timeout, None)
    }

    /// Cold repair preference, scoped to this client and its future reconnects.
    pub(crate) fn new_avoiding_peer(
        connect_timeout: Duration,
        avoid_peer: Option<IpAddr>,
    ) -> anyhow::Result<Self> {
        let trace = Arc::new(ConnectTrace::default());
        let resolver = TimedResolver {
            inner: GaiResolver::new(),
            trace: Arc::clone(&trace),
            avoid_peer,
            #[cfg(test)]
            fixed_answers: None,
        };
        Self::with_resolver(connect_timeout, trace, resolver)
    }

    fn with_resolver(
        connect_timeout: Duration, trace: Arc<ConnectTrace>, resolver: TimedResolver,
    ) -> anyhow::Result<Self> {
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
            connect_timeout,
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
            maintenance: Arc::new(MaintenancePriority::default()),
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
        self.maintenance.business.fetch_add(1, Ordering::SeqCst);
        let _business = BusinessRequest(&self.maintenance);
        if self.maintenance.active.load(Ordering::SeqCst) {
            self.maintenance.cancel.notify_waiters();
        }
        let gate_started = Instant::now();
        let deadline = tokio::time::Instant::now() + timeout;
        let _request_guard = match tokio::time::timeout_at(deadline, self.request_gate.acquire()).await {
            Ok(guard) => guard.expect("instrumented HTTP/1 request gate is never closed"),
            Err(_) => return Err(InstrumentedHttp1Error {
                kind: InstrumentedHttp1ErrorKind::QueueTimeout,
                message: format!("HTTP/1 slot wait exceeded {}ms; request not sent", timeout.as_millis()),
                // Do not read the active request's connect trace or retire its
                // healthy socket for a request that never acquired ownership.
                timings: Http1PhaseTimings {
                    slot_wait_ns: duration_ns(gate_started.elapsed()),
                    incomplete_phase: Http1IncompletePhase::SlotWait,
                    ..Http1PhaseTimings::default()
                },
            }),
        };
        let slot_wait_ns = duration_ns(gate_started.elapsed());
        self.request_on_gate(method, url, headers, body, deadline, slot_wait_ns).await
    }

    pub(crate) async fn keep_warm(&self, url: &str, timeout: Duration) -> KeepWarmOutcome {
        // Register before publishing active, then recheck business after
        // acquiring the gate. SeqCst closes the check/publish race in both
        // directions, including a business future queued before this probe.
        let cancelled = self.maintenance.cancel.notified();
        tokio::pin!(cancelled);
        cancelled.as_mut().enable();
        if self.maintenance.active.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return KeepWarmOutcome::Busy;
        }
        let _active = MaintenanceRequest(&self.maintenance);
        let Ok(_gate) = self.request_gate.try_acquire() else { return KeepWarmOutcome::Busy; };
        if self.maintenance.business.load(Ordering::SeqCst) != 0 {
            return KeepWarmOutcome::Busy;
        }
        tokio::select! {
            biased;
            _ = &mut cancelled => KeepWarmOutcome::Preempted,
            result = self.request_on_gate(reqwest::Method::GET, url,
                reqwest::header::HeaderMap::new(), Bytes::new(),
                tokio::time::Instant::now() + timeout, 0) => KeepWarmOutcome::Completed(result),
        }
    }

    async fn request_on_gate(&self, method: reqwest::Method, url: &str,
        headers: reqwest::header::HeaderMap, body: Bytes,
        deadline: tokio::time::Instant, slot_wait_ns: u64,
    ) -> Result<InstrumentedHttp1Response, InstrumentedHttp1Error> {
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
        let _io_guard = self.trace.io.begin();
        let headers_completed_ns = AtomicU64::new(0);
        self.trace.phase.store(PHASE_TTFB, Ordering::Release);
        let operation = async {
            let headers_started = Instant::now();
            let response = self
                .client
                .request(request)
                .await
                .map_err(|error| {
                    let kind = if error.is_connect() {
                        InstrumentedHttp1ErrorKind::Connect
                    } else {
                        InstrumentedHttp1ErrorKind::Transport
                    };
                    (kind, http_error_chain(&error))
                })?;
            let headers_ns = duration_ns(headers_started.elapsed());
            headers_completed_ns.store(headers_ns, Ordering::Release);
            if let Some(info) = response.extensions().get::<HttpInfo>() {
                self.trace.store_peer(info.remote_addr());
            }
            self.trace.phase.store(PHASE_BODY, Ordering::Release);
            let status = response.status();
            let body_started = Instant::now();
            let body = response
                .into_body()
                .collect()
                .await
                .map_err(|error| (InstrumentedHttp1ErrorKind::Transport, http_error_chain(&error)))?
                .to_bytes();
            let body_ns = duration_ns(body_started.elapsed());
            Ok::<_, (InstrumentedHttp1ErrorKind, String)>((status, body, headers_ns, body_ns))
        };
        match tokio::time::timeout_at(deadline, operation).await {
            Ok(Ok((status, body, headers_ns, body_ns))) => {
                let timings = self.snapshot(
                    attempts_before,
                    generation_before,
                    headers_ns,
                    body_ns,
                    duration_ns(started.elapsed()),
                    slot_wait_ns,
                    Http1IncompletePhase::None,
                );
                Ok(InstrumentedHttp1Response {
                    status,
                    body,
                    timings,
                })
            }
            Ok(Err((kind, message))) => {
                let total_ns = duration_ns(started.elapsed());
                let headers_ns = headers_completed_ns.load(Ordering::Acquire);
                Err(InstrumentedHttp1Error {
                    kind,
                    message,
                    timings: self.snapshot(
                        attempts_before,
                        generation_before,
                        if headers_ns == 0 {
                            total_ns
                        } else {
                            headers_ns
                        },
                        total_ns.saturating_sub(headers_ns).min(total_ns)
                            * u64::from(headers_ns != 0),
                        total_ns,
                        slot_wait_ns,
                        self.incomplete_phase(headers_ns),
                    ),
                })
            }
            Err(_) => {
                let total_ns = duration_ns(started.elapsed());
                let headers_ns = headers_completed_ns.load(Ordering::Acquire);
                Err(InstrumentedHttp1Error {
                    kind: InstrumentedHttp1ErrorKind::Timeout,
                    message: "HTTP/1.1 request deadline exceeded".to_owned(),
                    timings: self.snapshot(
                        attempts_before,
                        generation_before,
                        if headers_ns == 0 {
                            total_ns
                        } else {
                            headers_ns
                        },
                        total_ns.saturating_sub(headers_ns).min(total_ns)
                            * u64::from(headers_ns != 0),
                        total_ns,
                        slot_wait_ns,
                        self.incomplete_phase(headers_ns),
                    ),
                })
            }
        }
    }

    /// Published by the driver on stream retirement. Unknown/unconnected is
    /// distinct from a formerly warm generation that has closed.
    pub fn transport_closed(&self) -> bool {
        let generation = self.trace.generation.load(Ordering::Acquire);
        generation != 0 && self.trace.closed_generation.load(Ordering::Acquire) >= generation
    }

    pub fn connection_snapshot(&self) -> Http1ConnectionSnapshot {
        Http1ConnectionSnapshot {
            connect_generation: self.trace.generation.load(Ordering::Acquire),
            peer: self.trace.peer(),
        }
    }

    fn incomplete_phase(&self, headers_completed_ns: u64) -> Http1IncompletePhase {
        if headers_completed_ns != 0 {
            return Http1IncompletePhase::Body;
        }
        match self.trace.phase.load(Ordering::Acquire) {
            PHASE_DNS => Http1IncompletePhase::Dns,
            PHASE_TCP => Http1IncompletePhase::Tcp,
            PHASE_TLS => Http1IncompletePhase::Tls,
            PHASE_BODY => Http1IncompletePhase::Body,
            _ => Http1IncompletePhase::Ttfb,
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
        incomplete_phase: Http1IncompletePhase,
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
        let (dns_ns, tcp_ns, tls_ns, connect_worker_queue_ns) = if connect_attempted {
            let dns_ns = self.trace.dns_ns.load(Ordering::Acquire);
            let dns_tcp_ns = self.trace.dns_tcp_ns.load(Ordering::Acquire);
            let tls_total_ns = self.trace.tls_total_ns.load(Ordering::Acquire);
            let worker_queue = self.trace.connect_worker_queue_ns.load(Ordering::Acquire);
            (
                dns_ns,
                dns_tcp_ns.saturating_sub(dns_ns),
                tls_total_ns.saturating_sub(dns_tcp_ns).saturating_sub(worker_queue),
                worker_queue,
            )
        } else {
            (0, 0, 0, 0)
        };
        Http1PhaseTimings {
            io: self.trace.io.snapshot(if matches!(incomplete_phase, Http1IncompletePhase::None | Http1IncompletePhase::Body) { headers_ns } else { 0 }),
            connect_attempted,
            connect_generation_before: generation_before,
            connect_generation_after: generation_after,
            dns_ns,
            tcp_ns,
            tls_ns,
            connect_worker_queue_ns,
            ttfb_ns: headers_ns
                .saturating_sub(dns_ns)
                .saturating_sub(tcp_ns)
                .saturating_sub(tls_ns)
                .saturating_sub(connect_worker_queue_ns),
            body_ns,
            total_ns,
            slot_wait_ns,
            first_reuse_for_generation,
            peer: self.trace.peer(),
            incomplete_phase,
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "current_thread")]
    async fn cold_connect_runs_off_reactor_and_cancellation_drops_pending_work() {
        let reactor = std::thread::current().id();
        let worker = super::cold_connect(async { std::thread::current().id() }).await.unwrap();
        assert_ne!(reactor, worker);
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct DropFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) { self.0.store(true, std::sync::atomic::Ordering::Release); }
        }
        let flag = DropFlag(dropped.clone());
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(super::cold_connect(async move {
            let _flag = flag;
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        }));
        started_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !dropped.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cold_connect_worker_failure_returns_error_and_allows_next_connect() {
        assert!(super::cold_connect(async { panic!("controlled cold worker failure") }).await.is_err());
        assert_eq!(super::cold_connect(async { 17 }).await.unwrap(), 17);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_tls_handshake_has_a_connect_deadline_and_never_sends_http() {
        use super::*;
        use tokio::io::AsyncReadExt;
        // Startup cost is outside the deliberately short connection budget.
        cold_connect(async {}).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut hello = [0; 4096];
            assert!(stream.read(&mut hello).await.unwrap() > 0);
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let client = InstrumentedHttp1Client::new(Duration::from_millis(50)).unwrap();
        let error = client.request(reqwest::Method::GET, &format!("https://{addr}/time"),
            reqwest::header::HeaderMap::new(), Bytes::new(), Duration::from_secs(1))
            .await.unwrap_err();
        assert_eq!(error.kind, InstrumentedHttp1ErrorKind::Connect);
        assert_eq!(error.timings.incomplete_phase, Http1IncompletePhase::Tls);
        assert_eq!(error.timings.connect_generation_after, 0);
        assert_eq!(error.timings.io.written_bytes, 0);
        assert!(error.timings.connect_worker_queue_ns > 0);
        assert!(error.timings.total_ns < 1_000_000_000);
        tokio::time::timeout(Duration::from_secs(2), server).await.unwrap().unwrap();
    }

    #[test]
    #[ignore = "release: cold blocking-worker creation interference; no exchange/network traffic"]
    fn benchmark_cold_worker_creation_interference() {
        use super::*;
        const N: usize = 1_000;
        const REPAIRS: usize = 6;
        let cold_core = std::env::var("HEXPROBE_COLD_CORE").ok().map(|s| s.parse::<usize>().unwrap());
        if let Some(core) = cold_core {
            let mut cfg = hexagent_config::config::OsTuneConfig::default();
            cfg.background_cores = vec![core];
            crate::os_tune::init_from_config(&cfg);
        }
        crate::cold_connect_submit::prewarm().unwrap();
        for offload_submit in [false, true] {
            let mut values = Vec::with_capacity(N);
            let mut created = 0;
            for _ in 0..N {
                // A new runtime per round forces the initial empty blocking
                // pool, rather than concealing thread creation by prewarming it.
                let starts = Arc::new(AtomicU64::new(0));
                let worker_starts = starts.clone();
                let rt = tokio::runtime::Builder::new_current_thread().enable_all()
                    .on_thread_start(move || {
                        if let Some(core) = cold_core { assert!(core_affinity::set_for_current(core_affinity::CoreId { id: core })); }
                        worker_starts.fetch_add(1, Ordering::Relaxed);
                    }).build().unwrap();
                rt.block_on(async {
                    let mut repairs = Vec::with_capacity(REPAIRS);
                    for _ in 0..REPAIRS {
                        repairs.push(tokio::spawn(async move {
                            let future = async { tokio::time::sleep(Duration::from_millis(10)).await; };
                            if offload_submit { cold_connect(future).await.unwrap(); }
                            else {
                                let runtime = tokio::runtime::Handle::current();
                                let worker_runtime = runtime.clone();
                                runtime.spawn_blocking(move || worker_runtime.block_on(future)).await.unwrap();
                            }
                        }));
                    }
                    let enqueued = Instant::now();
                    let hot = tokio::spawn(async move { duration_ns(enqueued.elapsed()) });
                    values.push(hot.await.unwrap());
                    for task in repairs { task.await.unwrap(); }
                });
                created += starts.load(Ordering::Relaxed);
            }
            values.sort_unstable();
            println!("cold_submit_offload={offload_submit} n={N} median_ns={} p99_ns={} p999_ns={} max_ns={} created_workers={created} cold_inflight_bound={REPAIRS} submit_capacity=64 reply_capacity=1 boundary=hot_task_enqueue_to_first_poll fresh_blocking_pool_each_round=true",
                (values[N/2-1]+values[N/2])/2, values[N*99/100-1], values[N*999/1000-1], values[N-1]);
        }
        println!("cold_submit_final_stats depth_sampled_high_water_admitted_dequeued_rejected_panics={:?}", crate::cold_connect_submit::benchmark_snapshot());
    }

    #[test]
    #[ignore = "release: synthetic cold poll interference; no exchange/network traffic"]
    fn benchmark_cold_poll_interference() {
        use super::*;
        const N: usize = 2_000;
        const REPAIRS: usize = 6;
        let cold_core = std::env::var("HEXPROBE_COLD_CORE").ok().map(|s| s.parse::<usize>().unwrap());
        let rt = tokio::runtime::Builder::new_current_thread().enable_all()
            .on_thread_start(move || {
                if let Some(core) = cold_core { assert!(core_affinity::set_for_current(core_affinity::CoreId { id: core })); }
            }).build().unwrap();
        rt.block_on(async {
            // Warm the existing blocking pool before the measured boundary.
            let mut warm = Vec::new();
            for _ in 0..REPAIRS { warm.push(tokio::spawn(cold_connect(async { tokio::time::sleep(Duration::from_millis(10)).await; }))); }
            for task in warm { task.await.unwrap().unwrap(); }
            for offload in [false, true] {
                let mut values = Vec::with_capacity(N);
                for _ in 0..N {
                    let mut repairs = Vec::with_capacity(REPAIRS);
                    for _ in 0..REPAIRS {
                        repairs.push(tokio::spawn(async move {
                            let poll = async {
                                let start = Instant::now();
                                while start.elapsed() < Duration::from_micros(300) { std::hint::spin_loop(); }
                            };
                            if offload { cold_connect(poll).await.unwrap(); } else { poll.await; }
                        }));
                    }
                    let enqueued = Instant::now();
                    let hot = tokio::spawn(async move { duration_ns(enqueued.elapsed()) });
                    values.push(hot.await.unwrap());
                    for task in repairs { task.await.unwrap(); }
                }
                values.sort_unstable();
                println!("cold_offload={offload} n={N} median_ns={} p99_ns={} p999_ns={} max_ns={} cold_inflight_high_water={REPAIRS} reply_capacity=1 overflow=0 boundary=hot_task_enqueue_to_first_poll synthetic_cold_polls=6x300us",
                    (values[N/2-1]+values[N/2])/2, values[N*99/100-1], values[N*999/1000-1], values[N-1]);
            }
        });
    }
    use super::*;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[test]
    fn repair_dns_excludes_failed_peer_only_when_an_alternative_exists() {
        let a: SocketAddr = "192.0.2.1:443".parse().unwrap();
        let b: SocketAddr = "192.0.2.2:443".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::1]:443".parse().unwrap();
        assert_eq!(repair_addresses([a, b, a, v6].into_iter(), Some(a.ip())).collect::<Vec<_>>(), [b, v6]);
        assert_eq!(repair_addresses([a, b].into_iter(), None).collect::<Vec<_>>(), [a, b]);
        assert_eq!(repair_addresses([a].into_iter(), Some(a.ip())).collect::<Vec<_>>(), [a]);
        assert_eq!(repair_addresses([v6, b].into_iter(), Some(v6.ip())).collect::<Vec<_>>(), [b]);
        assert_eq!(repair_addresses([].into_iter(), Some(a.ip())).count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fresh_client_time_success_does_not_hide_failed_peer_on_next_post() {
        use tokio::net::TcpListener;
        let bad = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bad_addr = bad.local_addr().unwrap();
        let good = TcpListener::bind((std::net::Ipv6Addr::LOCALHOST, bad_addr.port())).await.unwrap();
        let good_addr = good.local_addr().unwrap();
        async fn request(stream: &mut tokio::net::TcpStream) -> String {
            let mut data = Vec::new();
            loop {
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                data.push(byte[0]);
                if data.ends_with(b"\r\n\r\n") { break; }
                assert!(data.len() < 4096);
            }
            String::from_utf8(data).unwrap()
        }
        let bad_server = tokio::spawn(async move {
            // Both the initial client and an ordinary fresh replacement choose
            // this same DNS answer. /time works; the real route drops the socket.
            for _ in 0..2 {
                let (mut stream, _) = bad.accept().await.unwrap();
                assert!(request(&mut stream).await.starts_with("GET /time "));
                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").await.unwrap();
                let post = request(&mut stream).await;
                assert!(post.starts_with("POST /order "));
                assert!(post.to_ascii_lowercase().contains("host: repair.test:"));
                drop(stream);
            }
        });
        let good_server = tokio::spawn(async move {
            let (mut stream, _) = good.accept().await.unwrap();
            for method in ["GET /time ", "POST /order "] {
                assert!(request(&mut stream).await.starts_with(method));
                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}").await.unwrap();
            }
        });
        for avoid in [None, None, Some(bad_addr.ip())] {
            let trace = Arc::new(ConnectTrace::default());
            let resolver = TimedResolver { inner: GaiResolver::new(), trace: Arc::clone(&trace),
                avoid_peer: avoid, fixed_answers: Some(vec![bad_addr, good_addr]) };
            let client = InstrumentedHttp1Client::with_resolver(Duration::from_secs(1), trace, resolver).unwrap();
            let root = format!("http://repair.test:{}", bad_addr.port());
            let warm = client.request(reqwest::Method::GET, &format!("{root}/time"),
                reqwest::header::HeaderMap::new(), Bytes::new(), Duration::from_secs(2)).await.unwrap();
            assert_eq!(warm.timings.connect_generation_after, 1);
            let reply = client.request(reqwest::Method::POST, &format!("{root}/order"),
                reqwest::header::HeaderMap::new(), Bytes::new(), Duration::from_secs(2)).await;
            if avoid.is_some() {
                let reply = reply.unwrap();
                assert_eq!(reply.timings.peer, Some(good_addr));
                assert!(!reply.timings.connect_attempted);
            } else {
                let error = reply.unwrap_err();
                assert_eq!(error.timings.peer, Some(bad_addr));
                assert_eq!(error.timings.io.read_bytes, 0);
                assert!(error.timings.io.written_bytes > 0);
            }
        }
        bad_server.await.unwrap();
        good_server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "release: HTTP/1 loopback completion, instrumented versus bare Hyper reference"]
    async fn benchmark_http_io_boundaries() {
        const N: usize = 2_000;
        async fn server() -> (String, tokio::task::JoinHandle<()>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let task = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                let mut buf = [0_u8; 2048];
                for _ in 0..N + 32 {
                    let mut used = 0;
                    while !buf[..used].windows(4).any(|w| w == b"\r\n\r\n") {
                        let count = stream.read(&mut buf[used..]).await.unwrap();
                        assert!(count > 0);
                        used += count;
                    }
                    stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok").await.unwrap();
                }
            });
            (url, task)
        }
        let (plain_url, plain_server) = server().await;
        let (traced_url, traced_server) = server().await;
        let mut http = HttpConnector::new();
        http.set_nodelay(true);
        let mut builder = Client::builder(TokioExecutor::new());
        builder.retry_canceled_requests(false).pool_max_idle_per_host(1);
        let plain: Client<_, Full<Bytes>> = builder.build(http);
        let traced = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        let mut ns = [Vec::with_capacity(N), Vec::with_capacity(N)];
        for i in 0..N + 32 {
            for mode in [i % 2, 1 - i % 2] {
                let start = Instant::now();
                if mode == 0 {
                    let request = Request::builder().uri(&plain_url).body(Full::new(Bytes::new())).unwrap();
                    std::hint::black_box(plain.request(request).await.unwrap().into_body().collect().await.unwrap());
                } else {
                    let response = traced.request(reqwest::Method::GET, &traced_url, reqwest::header::HeaderMap::new(), Bytes::new(), Duration::from_secs(1)).await.unwrap();
                    assert!(response.timings.io.first_read_offset_ns > 0);
                    std::hint::black_box(response);
                }
                if i >= 32 { ns[mode].push(duration_ns(start.elapsed())); }
            }
        }
        plain_server.await.unwrap(); traced_server.await.unwrap();
        for (mode, values) in ns.iter_mut().enumerate() {
            values.sort_unstable();
            eprintln!("http_io_probe mode={} boundary=request_build_through_body_completion N={N} p50_ns={} p99_ns={} p999_ns={} max_ns={} in_flight=1 queued=0 overflow=0", ["bare_hyper_reference", "instrumented"][mode], values[999], values[1979], values[1997], values[1999]);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plaintext_boundaries_separate_server_wait_partial_headers_and_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let count = stream.read(&mut request).await.unwrap();
            assert!(count > 0);
            tokio::time::sleep(Duration::from_millis(25)).await;
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n").await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            stream.write_all(b"Connection: close\r\n\r\n").await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            stream.write_all(b"ok").await.unwrap();
        });
        let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        let response = client.request(reqwest::Method::GET, &format!("http://{addr}/"),
            reqwest::header::HeaderMap::new(), Bytes::new(), Duration::from_secs(1)).await.unwrap();
        let t = response.timings;
        assert!(t.io.response_wait_ns >= 20_000_000, "{:?}", t);
        assert!(t.io.header_decode_ns >= 15_000_000, "{:?}", t);
        assert!(t.body_ns >= 15_000_000, "{:?}", t);
        assert!(t.io.flush_offset_ns >= t.io.first_write_offset_ns);
        assert!(t.io.first_read_offset_ns >= t.io.flush_offset_ns);
        assert_eq!(response.body, Bytes::from_static(b"ok"));
        #[cfg(target_os = "linux")]
        assert!(t.io.tcp_sampled, "successful loopback request needs both TCP_INFO samples");
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refused_connection_is_not_sent_but_lost_response_stays_ambiguous() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let n = stream.read(&mut request).await.unwrap();
            assert!(n > 0); // Server saw the POST, then lost its response.
            assert!(request[..n].starts_with(b"POST "));
        });
        let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        let url = format!("http://{addr}/order");
        let error = client.request(reqwest::Method::POST, &url,
            reqwest::header::HeaderMap::new(), Bytes::from_static(b"signed-order"),
            Duration::from_secs(1)).await.unwrap_err();
        server.await.unwrap();
        assert_eq!(error.kind, InstrumentedHttp1ErrorKind::Transport);
        assert!(error.message.contains(": "), "underlying cause is retained: {}", error.message);
        // Listener is now gone; the connector can prove this second request
        // never reached an HTTP connection. No inferred elapsed-time cutoff.
        let error = client.request(reqwest::Method::POST, &url,
            reqwest::header::HeaderMap::new(), Bytes::from_static(b"signed-order"),
            Duration::from_secs(1)).await.unwrap_err();
        assert_eq!(error.kind, InstrumentedHttp1ErrorKind::Connect);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_request_deadline_expires_without_sending_or_touching_active_trace() {
        let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        let guard = client.request_gate.acquire().await.unwrap();
        client.trace.generation.store(7, Ordering::Relaxed);
        client.trace.phase.store(PHASE_BODY, Ordering::Relaxed);
        let error = client.request(reqwest::Method::POST, "http://127.0.0.1:1/order",
            reqwest::header::HeaderMap::new(), Bytes::from_static(b"must-not-send"),
            Duration::from_millis(10)).await.unwrap_err();
        assert_eq!(error.kind, InstrumentedHttp1ErrorKind::QueueTimeout);
        assert_eq!(error.timings.incomplete_phase, Http1IncompletePhase::SlotWait);
        assert_eq!(error.timings.total_ns, 0);
        assert!(error.timings.slot_wait_ns >= 10_000_000);
        assert_eq!(client.trace.attempts.load(Ordering::Relaxed), 0);
        assert_eq!(client.trace.phase.load(Ordering::Relaxed), PHASE_BODY);
        assert_eq!(client.trace.generation.load(Ordering::Relaxed), 7);
        drop(guard);
        assert_eq!(client.request_gate.available_permits(), 1);
    }

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

    #[test]
    fn retiring_socket_watermark_does_not_invalidate_new_generation() {
        let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        assert!(!client.transport_closed());
        client.trace.generation.store(2, Ordering::Release);
        let socket = |generation| TimedIo {
            inner: (), trace: Arc::clone(&client.trace), generation, flush_pending: false,
            #[cfg(target_os = "linux")]
            socket_fd: -1,
        };
        drop(socket(1));
        assert!(!client.transport_closed());
        drop(socket(2));
        assert!(client.transport_closed());
        client.trace.generation.store(3, Ordering::Release);
        assert!(!client.transport_closed());
        drop(socket(1));
        assert!(!client.transport_closed());
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

        tokio::time::timeout(Duration::from_secs(1), async {
            while !client.transport_closed() { tokio::task::yield_now().await; }
        }).await.expect("Connection: close must retire idle generation without another request");

        let reconnected = request().await.unwrap();
        assert!(reconnected.timings.transparent_reconnect());
        assert!(!reconnected.timings.first_reuse_for_generation);
        assert_eq!(reconnected.timings.connect_generation_before, 1);
        assert_eq!(reconnected.timings.connect_generation_after, 2);
        for response in [&initial, &reused, &reconnected] {
            let io = response.timings.io;
            assert!(io.written_bytes > 0 && io.read_bytes > 0);
            assert!(io.first_write_offset_ns > 0);
            assert!(io.first_read_offset_ns >= io.first_write_offset_ns);
            assert!(io.response_wait_ns > 0);
            assert!(io.first_read_offset_ns <= response.timings.total_ns);
        }
        assert_eq!(initial.timings.io.written_bytes, reused.timings.io.written_bytes);
        assert_eq!(reused.timings.io.written_bytes, reconnected.timings.io.written_bytes);
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

    #[tokio::test(flavor = "current_thread")]
    async fn timeout_retains_peer_generation_and_incomplete_ttfb_stage() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            tokio::time::sleep(Duration::from_millis(80)).await;
        });
        let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        let error = client
            .request(
                reqwest::Method::GET,
                &format!("http://{addr}/time"),
                reqwest::header::HeaderMap::new(),
                Bytes::new(),
                Duration::from_millis(20),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, InstrumentedHttp1ErrorKind::Timeout);
        assert_eq!(error.timings.incomplete_phase, Http1IncompletePhase::Ttfb);
        assert_eq!(error.timings.peer.map(|peer| peer.ip()), Some(addr.ip()));
        assert_eq!(error.timings.connect_generation_after, 1);
        // The absolute 20ms budget includes cold connect/worker pickup.
        // Check the observed phase and total deadline, without assuming
        // connection establishment always consumed less than 5ms.
        assert!(error.timings.total_ns >= 20_000_000);
        assert!(error.timings.ttfb_ns > 0);
        assert!(error.timings.ttfb_ns <= error.timings.total_ns);
        server.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn timeout_after_headers_is_attributed_to_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            stream.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(80)).await;
        });
        let client = InstrumentedHttp1Client::new(Duration::from_secs(1)).unwrap();
        let error = client
            .request(
                reqwest::Method::GET,
                &format!("http://{addr}/time"),
                reqwest::header::HeaderMap::new(),
                Bytes::new(),
                Duration::from_millis(20),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, InstrumentedHttp1ErrorKind::Timeout);
        assert_eq!(error.timings.incomplete_phase, Http1IncompletePhase::Body);
        // The deadline also includes connect/header time, which can exceed
        // 5 ms on a loaded test host. Verify the boundary, not that assumption.
        assert!(error.timings.body_ns > 0);
        assert!(error.timings.body_ns < error.timings.total_ns);
        assert!(error.timings.io.first_read_offset_ns > 0);
        server.await.unwrap();
    }
}

#[cfg(test)]
mod keep_warm_tests;
