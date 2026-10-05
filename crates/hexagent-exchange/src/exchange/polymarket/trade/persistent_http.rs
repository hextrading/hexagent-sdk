//! One persistent HTTP actor per physical execution owner, on the order reactor.
//! Mailboxes retain two commands; the physical owner admits one business request
//! at a time. Queue rejection is explicitly NotSent, never an uncertain POST.
use super::*;

#[derive(Clone)]
pub struct HttpOrderLane(hexagent_runtime::root_owner::Sender<Command>);
impl HttpOrderLane {
    pub fn new() -> Result<Self> {
        let registry =
            async_rt::order_owners().ok_or_else(|| anyhow!("order reactor not initialized"))?;
        let sender = registry
            .register(
                2,
                |mut inbox: hexagent_runtime::root_owner::Inbox<Command>| async move {
                    while let Some(command) = inbox.recv().await {
                        command.run().await;
                    }
                },
            )
            .map_err(|error| anyhow!(error))?;
        Ok(Self(sender))
    }
}

pub(super) struct Dispatch {
    pub lane: HttpOrderLane,
    pub context: Arc<Context>,
}
pub(super) struct Context {
    iid_a: String,
    account_id: String,
    auth_failure_blocked: Arc<std::sync::atomic::AtomicBool>,
    request_buffers: Arc<ArrayQueue<BytesMut>>,
    phase_audit: Arc<HttpPhaseAudit>,
}
impl Context {
    pub fn new(shared: &SharedState, instance_id: &str) -> Arc<Self> {
        Arc::new(Self {
            iid_a: instance_id.to_owned(),
            account_id: shared.account_state.account_id().to_owned(),
            auth_failure_blocked: shared.auth_failure_blocked.clone(),
            request_buffers: shared.request_buffers.clone(),
            phase_audit: shared.http_phase_audit.clone(),
        })
    }
}

pub(super) struct Command {
    pub context: Arc<Context>,
    pub client: crate::http1_pool::PooledClient,
    pub attempt_id: u64,
    pub method_a: reqwest::Method,
    pub path_a: std::borrow::Cow<'static, str>,
    pub body_a: Bytes,
    pub url_a: Arc<str>,
    pub headers: super::super::auth::AuthHeaders,
    pub tx_a: HttpReplySender,
    pub timing_a: Arc<HttpCompletionTiming>,
    pub phase_kind: &'static str,
    pub rec_kind: Option<crate::latency_record::RequestKind>,
    pub stage: &'static str,
    pub runtime_queue_stage: &'static str,
    pub network_stage: &'static str,
    pub reply_enqueue_stage: &'static str,
    pub t_start: crate::latency::Instant,
    pub enqueued_at: crate::latency::Instant,
    pub peer_failure_observer: Option<(
        crate::http1_pool::PooledClient,
        super::super::execution_peer_failure::PeerFailureSender,
    )>,
}
impl Command {
    pub async fn run(self) {
        let Self {
            context,
            client,
            attempt_id,
            method_a,
            path_a,
            body_a,
            url_a,
            headers,
            tx_a,
            timing_a,
            phase_kind,
            rec_kind,
            stage,
            runtime_queue_stage,
            network_stage,
            reply_enqueue_stage,
            t_start,
            enqueued_at,
            peer_failure_observer,
        } = self;
        let Context {
            iid_a,
            account_id,
            auth_failure_blocked,
            request_buffers,
            phase_audit,
        } = context.as_ref();

        let runtime_queue_ns = enqueued_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        crate::latency::record(runtime_queue_stage, enqueued_at);
        let network_started = crate::latency::Instant::now();
        // Keep one cheap Bytes handle so the unique allocation can be
        // recovered and returned to the startup-filled pool after
        // reqwest drops its request body.
        let recyclable = body_a.clone();
        let reply = execute_http_with_cancel_connection_failure_hedge(
            client,
            attempt_id,
            &account_id,
            &method_a,
            &url_a,
            &path_a,
            &headers,
            body_a,
            &phase_audit,
            HttpPhaseContext {
                root_attempt_id: attempt_id,
                leg: 0,
                kind: phase_kind,
                runtime_queue_ns,
            },
        )
        .await;
        report_cold_http_peer_failure(peer_failure_observer, &reply);
        observe_authenticated_reply_gate(&reply, &account_id, auth_failure_blocked.as_ref());
        if let Ok(mut buffer) = recyclable.try_into_mut() {
            buffer.clear();
            let _ = request_buffers.push(buffer);
        }
        crate::latency::record(network_stage, network_started);
        timing_a
            .response_ready_ns
            .store(now_ns(), Ordering::Release);
        let rec = rec_kind
            .filter(|_| crate::latency_record::is_active())
            .map(|k| (k, latency_record_status(&reply)));
        let reply_enqueue_started = crate::latency::Instant::now();
        if tx_a.try_send(reply).is_ok() {
            timing_a
                .reply_enqueued_ns
                .store(now_ns(), Ordering::Release);
            crate::latency::record(reply_enqueue_stage, reply_enqueue_started);
            crate::latency::record(stage, t_start);
            if let Some((k, status)) = rec {
                crate::latency_record::record(
                    &iid_a,
                    k,
                    t_start.elapsed().as_secs_f64() * 1000.0,
                    status,
                );
            }
        }
    }
    pub fn reject(self) {
        if let Ok(mut buffer) = self.body_a.try_into_mut() {
            buffer.clear();
            let _ = self.context.request_buffers.push(buffer);
        }
        let _ = self.tx_a.try_send(Err(HttpErr::NotSent(
            "persistent I/O mailbox full or stopped".into(),
        )));
    }
}
impl Dispatch {
    pub fn send(&self, command: Command) {
        if let Err(error) = self.lane.0.try_send(command) {
            error.into_inner().reject();
        }
    }
}
