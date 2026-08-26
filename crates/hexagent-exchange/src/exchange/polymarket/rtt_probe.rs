//! RTT-probe task — synthetic latency measurement via real signed orders.
//!
//! Live partner of polymaker's apv2 quote_n/PROBE channel (formerly the
//! deleted `rtt_gate` module). While the strategy
//! sits in PROBE mode (no quoting), this task fires `POST /order`
//! place probes at a fixed cadence (default 2 s) and pushes the
//! round-trip duration back to the strategy via a crossbeam channel.
//!
//! ## Probe design (resting place + cancel)
//!
//! **place leg**: builds a fully-signed `POST /order` body the same way
//! `PolymarketTrade::sign_and_build_body_v2` does for real submits —
//! same auth, same EIP-712 hash, same wire shape. The order is a
//! `postOnly BUY` of the high-priced side at the deep
//! [`FULL_PROBE_PRICE`] (0.01) for [`FULL_PROBE_SIZE`] shares — notional
//! comfortably above the per-market `min_size`, so the place is
//! **accepted and rests**. Deep + `postOnly` + high-side means it never
//! fills (it can't cross, and the high-side choice keeps 0.01 far below
//! that token's book). RTT covers exactly the accept→rest code path a
//! real maker submit hits.
//!
//! **cancel leg**: a targeted `DELETE /order` against the resting
//! order's id, fired right after the place so the ~$1 of reserved
//! collateral is released within a few ms. Its latency is sampled too.
//!
//! ## Why a *resting* order (vs the older reject / place-only probes)
//!
//! Two earlier designs biased RTT low. (1) A `qty=1` min-size *reject*
//! short-circuits at validation, before the accept→rest matching path a
//! real maker submit exercises. (2) Place-only with no resting order
//! left the cancel leg hitting 404s (~30 ms p95 — server short-circuits
//! at auth + orderID-lookup) while real place RTT sat at 1500-2000 ms,
//! suppressing p95 and blinding the gate. A real *resting* order fixes
//! both: the place exercises accept→rest, and the cancel targets a
//! genuine order id (a real matching-engine `DELETE`, not a 404), so
//! both legs track the live `place_order` / `cancel_order` distributions.
//!
//! ## Why not `DELETE /cancel-all` (the original design)
//!
//! Polymarket short-circuits cancel-all against an empty book at the
//! auth+route layer with essentially no matching-engine work. RTT
//! samples were systematically 2-3× faster than the real
//! `place_order` / `cancel_order` distributions the gate is supposed
//! to track.
//!
//! ## Active token availability
//!
//! Place probe needs a real `clob_token_id` to address. The strategy
//! (or, in RECORD mode, the recorder loop) stashes the current event's
//! **high-priced side** token id into a shared
//! atomic immutable snapshot and refreshes it as the book moves,
//! clearing it on settlement. Probe reads never lock the strategy
//! thread. When `None` (no active event in this
//! series), the place probe is skipped — no fallback (cold start and
//! inter-event gaps push zero samples until the next event).
//!
//! ## Up/Down side selection ([`pick_probe_side`])
//!
//! The probe always buys at the fixed deep [`FULL_PROBE_PRICE`] (0.01)
//! so the order rests far below the book and never fills. In a binary
//! Up/Down market the two sides' prices are ~complementary (sum ≈ 1.0),
//! so exactly one side trades high (best ask near 1.0) and the other
//! cheap (best ask toward the 0.01 floor). Buying the **cheap** side at
//! 0.01 risks sitting at / crossing the top → `postOnly` rejects it,
//! which short-circuits *before* the accept→rest matching path and
//! biases RTT low — the very failure the resting-place redesign exists
//! to avoid. So the upstream writer picks whichever side currently has
//! the higher best ask; the probe just buys whatever token it's handed.
//!
//! ## Failure handling
//!
//! * Server responded (200, 400 minSize, 5xx, 425) — RTT recorded.
//!   A rejected place additionally WARNs with the response body,
//!   rate-limited to once per minute: rejection means the probe has
//!   degraded to the reject-RTT shape above and its samples are
//!   biased low, which is otherwise invisible outside the CSV.
//! * **Timeout / status-less transport failure / malformed 2xx body**
//!   (`HttpErr::Timeout` / `HttpErr::Transport` /
//!   `HttpErr::InvalidResponse`) — recorded with the elapsed time as the sample.
//!   These are primary failure modes the gate exists to detect; suppressing
//!   them would blind the gate to network degradation. The locally-computed
//!   order hash is still cancelled because the placement may have landed.
//! * Pre-dispatch/local failures (`HttpErr::Other`) — skipped. They are not
//!   representative of submit transport latency.
//!
//! Per-call timeouts are bounded by the FAST h2 client pool ceiling
//! (typically 1500–2000 ms via `async_rt::current_fast_timeout`).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use crossbeam_channel::{Receiver, Sender};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use super::trade::{
    probe_cancel_response_is_terminal, HttpErr, PolymarketTrade, ProbeReconcileOutcome, SharedState,
};

const PROBE_ORPHAN_OWNER_CAPACITY: usize = 64;

/// Durable operational state for a synthetic RTT request whose place result
/// was ambiguous. This intentionally lives outside StrategyAccount's economic
/// reservations: probes must never make strategy buying power appear spent.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ProbeOrphan {
    pub instance_id: String,
    pub client_order_id: String,
    pub order_id: String,
    pub token_id: String,
    pub created_at_ns: u64,
    /// Probe-only operational collateral in USDC micros. It never enters a
    /// strategy reservation ledger; resolving this record is the idempotent
    /// release of the separate probe reservation.
    #[serde(default)]
    pub reserved_cash_micros: u64,
    #[serde(default)]
    pub parallel_absence_observations: u8,
}

#[derive(Default, Deserialize, Serialize)]
struct ProbeOrphanFile {
    version: u32,
    pending: Option<ProbeOrphan>,
}

enum ProbeOrphanCommand {
    Acquire(ProbeOrphan, Sender<Result<bool, String>>),
    Current(Sender<Option<ProbeOrphan>>),
    Resolve {
        order_id: String,
        reply: Sender<Result<bool, String>>,
    },
    NoteParallelAbsence {
        order_id: String,
        reply: Sender<Result<u8, String>>,
    },
}

/// Bounded MPSC control handle; a single low-priority owner is the only writer
/// of the sidecar and in-memory orphan. The account-level singleton is shared
/// by every strategy instance, so at most one token/account probe is pending.
#[derive(Clone)]
pub(crate) struct ProbeOrphanOwner {
    tx: Sender<ProbeOrphanCommand>,
    high_water: Arc<AtomicUsize>,
    overflow: Arc<AtomicU64>,
}

impl ProbeOrphanOwner {
    fn request<T>(
        &self,
        command: impl FnOnce(Sender<T>) -> ProbeOrphanCommand,
    ) -> Result<T, String> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let depth = self
            .tx
            .len()
            .saturating_add(1)
            .min(PROBE_ORPHAN_OWNER_CAPACITY);
        self.tx
            .send_timeout(command(reply_tx), Duration::from_secs(2))
            .map_err(|error| {
                self.overflow.fetch_add(1, Ordering::Relaxed);
                format!("probe orphan owner unavailable: {error}")
            })?;
        self.high_water.fetch_max(depth, Ordering::Relaxed);
        reply_rx
            .recv_timeout(Duration::from_secs(2))
            .map_err(|error| format!("probe orphan owner reply unavailable: {error}"))
    }

    pub(crate) fn acquire(&self, orphan: ProbeOrphan) -> Result<bool, String> {
        self.request(|reply| ProbeOrphanCommand::Acquire(orphan, reply))?
    }

    pub(crate) fn current(&self) -> Result<Option<ProbeOrphan>, String> {
        self.request(ProbeOrphanCommand::Current)
    }

    pub(crate) fn resolve(&self, order_id: &str) -> Result<bool, String> {
        let order_id = order_id.to_string();
        self.request(|reply| ProbeOrphanCommand::Resolve { order_id, reply })?
    }

    pub(crate) fn note_parallel_absence(&self, order_id: &str) -> Result<u8, String> {
        let order_id = order_id.to_string();
        self.request(|reply| ProbeOrphanCommand::NoteParallelAbsence { order_id, reply })?
    }

    pub(crate) fn metrics(&self) -> (usize, u64) {
        (
            self.high_water.load(Ordering::Relaxed),
            self.overflow.load(Ordering::Relaxed),
        )
    }
}

fn probe_orphan_sidecar_path(ledger_path: &Path) -> PathBuf {
    let name = ledger_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("account-ledger");
    ledger_path.with_file_name(format!("{name}.probe-orphans.json"))
}

fn load_probe_orphan(path: Option<&Path>) -> Result<Option<ProbeOrphan>, String> {
    let Some(path) = path else { return Ok(None) };
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let file: ProbeOrphanFile = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse {}: {error}", path.display()))?;
    Ok(file.pending)
}

fn persist_probe_orphan(path: Option<&Path>, pending: &Option<ProbeOrphan>) -> Result<(), String> {
    let Some(path) = path else { return Ok(()) };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)
        .map_err(|error| format!("create {}: {error}", parent.display()))?;
    let temp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(&ProbeOrphanFile {
        version: 1,
        pending: pending.clone(),
    })
    .map_err(|error| format!("serialize probe orphan: {error}"))?;
    {
        use std::io::Write as _;
        let mut file = std::fs::File::create(&temp)
            .map_err(|error| format!("create {}: {error}", temp.display()))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| format!("sync {}: {error}", temp.display()))?;
    }
    std::fs::rename(&temp, path).map_err(|error| format!("rename {}: {error}", path.display()))?;
    if let Ok(parent_file) = std::fs::File::open(parent) {
        let _ = parent_file.sync_all();
    }
    Ok(())
}

pub(crate) fn spawn_probe_orphan_owner(
    ledger_path: Option<&Path>,
    shutdown: hexagent_runtime::shutdown::ShutdownToken,
) -> std::io::Result<(ProbeOrphanOwner, std::thread::JoinHandle<()>)> {
    let path = ledger_path.map(probe_orphan_sidecar_path);
    let initial = load_probe_orphan(path.as_deref()).unwrap_or_else(|error| {
        log::error!("[RttProbe] durable orphan load failed; owner starts blocked: {error}");
        Some(ProbeOrphan {
            instance_id: "<load-error>".into(),
            client_order_id: "probe:load-error".into(),
            order_id: "<unknown>".into(),
            token_id: String::new(),
            created_at_ns: crate::types::now_ns(),
            reserved_cash_micros: 0,
            parallel_absence_observations: 0,
        })
    });
    let (tx, rx) = crossbeam_channel::bounded(PROBE_ORPHAN_OWNER_CAPACITY);
    let owner = ProbeOrphanOwner {
        tx,
        high_water: Arc::new(AtomicUsize::new(0)),
        overflow: Arc::new(AtomicU64::new(0)),
    };
    let join = std::thread::Builder::new()
        .name("poly-probe-orphan-owner".into())
        .spawn(move || run_probe_orphan_owner(rx, path, initial, shutdown))?;
    Ok((owner, join))
}

fn run_probe_orphan_owner(
    rx: Receiver<ProbeOrphanCommand>,
    path: Option<PathBuf>,
    mut pending: Option<ProbeOrphan>,
    shutdown: hexagent_runtime::shutdown::ShutdownToken,
) {
    crate::os_tune::pin_background("poly-probe-orphan-owner");
    while !shutdown.is_finished() {
        let command = match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(command) => command,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        };
        match command {
            ProbeOrphanCommand::Acquire(orphan, reply) => {
                let result = if pending.is_some() {
                    Ok(false)
                } else {
                    let next = Some(orphan);
                    persist_probe_orphan(path.as_deref(), &next).map(|()| {
                        pending = next;
                        true
                    })
                };
                let _ = reply.send(result);
            }
            ProbeOrphanCommand::Current(reply) => {
                let _ = reply.send(pending.clone());
            }
            ProbeOrphanCommand::Resolve { order_id, reply } => {
                let matched = pending
                    .as_ref()
                    .is_some_and(|orphan| orphan.order_id.eq_ignore_ascii_case(&order_id));
                let result = if matched {
                    persist_probe_orphan(path.as_deref(), &None).map(|()| {
                        pending = None;
                        true
                    })
                } else {
                    Ok(false)
                };
                let _ = reply.send(result);
            }
            ProbeOrphanCommand::NoteParallelAbsence { order_id, reply } => {
                let result = if let Some(orphan) = pending
                    .as_mut()
                    .filter(|orphan| orphan.order_id.eq_ignore_ascii_case(&order_id))
                {
                    orphan.parallel_absence_observations =
                        orphan.parallel_absence_observations.saturating_add(1);
                    let observations = orphan.parallel_absence_observations;
                    persist_probe_orphan(path.as_deref(), &pending).map(|()| observations)
                } else {
                    Ok(0)
                };
                let _ = reply.send(result);
            }
        }
    }
}

/// Probe resting-order parameters. A postOnly `BUY` of the high-priced
/// side (see [`pick_probe_side`]) at this deep price never crosses the
/// book, so it always rests (so it CAN be cancelled) and never fills
/// (postOnly rejects any taking fill, and 0.01 sits far below the
/// high-side book). The size (100) clears the 5-share floor and puts the
/// notional (`price × size`) at Polymarket's ~$1 per-order minimum
/// (100 × 0.01 = $1.00) so the place is accepted; ~$1 of collateral is
/// reserved for the few-ms the order rests before the cancel releases it.
/// NOTE: at the $1 floor — if a market's min is enforced as strictly
/// `> $1`, bump `FULL_PROBE_SIZE` or `FULL_PROBE_PRICE` so the place
/// keeps resting (a rejected place falls back to a 404 cancel and biases
/// RTT low — the failure the resting-probe design avoids).
const FULL_PROBE_PRICE: f64 = 0.01;
const FULL_PROBE_SIZE: f64 = 100.0;

/// Rate limit for the probe-place-rejected WARN: a healthy probe is
/// never rejected (the resting design depends on the place being
/// accepted), so a persistent reject stream means the probe has
/// degraded to the reject-probe shape and its RTT samples are biased
/// low. That failure is silent at the gate — surface it, but at most
/// once per this window (the probe fires every ~2 s; unthrottled this
/// would WARN 43k×/day, as the 2026-07 poly_1271 signing regression
/// did at INFO-invisible level).
const REJECT_WARN_EVERY_SECS: u64 = 60;
static LAST_REJECT_WARN_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static REJECTS_SINCE_WARN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// WARN (rate-limited) that a probe place was rejected, with the HTTP
/// status and (truncated) response body so the reject *reason* lands in
/// the log — `HttpErr::Status` bodies are otherwise dropped here and
/// the degradation is only visible in the latency CSV status column.
fn warn_probe_place_rejected(code: u16, body: &str) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    REJECTS_SINCE_WARN.fetch_add(1, Ordering::Relaxed);
    let last = LAST_REJECT_WARN_SECS.load(Ordering::Relaxed);
    if now.saturating_sub(last) < REJECT_WARN_EVERY_SECS {
        return;
    }
    if LAST_REJECT_WARN_SECS
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return; // another thread just warned
    }
    let n = REJECTS_SINCE_WARN.swap(0, Ordering::Relaxed);
    let body_short: String = body.chars().take(200).collect();
    warn!(
        "[RttProbe] place REJECTED http_{} ({} rejects in last {}s) — probe is degraded \
         to reject-RTT (biased low): {}",
        code, n, REJECT_WARN_EVERY_SECS, body_short,
    );
}

/// Strategy → probe handoff for the current event's probe-target token
/// (the high-priced binary side; see [`pick_probe_side`]). `Some(token)`
/// when an event is active in the polymaker series; `None` otherwise.
/// Probe reads on every place cycle; the writer (strategy or recorder)
/// sets it at event start and refreshes it as the book moves, clearing
/// it on settlement.
#[derive(Clone)]
pub struct ActiveTokenHandle {
    value: Arc<ArcSwapOption<String>>,
}

impl ActiveTokenHandle {
    pub fn new(initial: Option<String>) -> Self {
        Self {
            value: Arc::new(ArcSwapOption::from(initial.map(Arc::new))),
        }
    }

    /// Publish the current probe token with one atomic pointer swap. Token
    /// rotation is control-plane work; periodic probe reads never contend
    /// with the strategy's market-data callback.
    pub fn store(&self, value: Option<String>) {
        self.value.store(value.map(Arc::new));
    }

    pub fn load(&self) -> Option<Arc<String>> {
        self.value.load_full()
    }
}

impl Default for ActiveTokenHandle {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Choose which side of a binary Up/Down market the probe should target
/// so its deep `BUY @ FULL_PROBE_PRICE` rests far below the book.
///
/// Picks the side with the **higher best ask** (closest to 1.0), which
/// maximizes the gap between 0.01 and the top — the resting headroom.
/// When only one side's ask is known, uses binary complementarity (the
/// other side ≈ 1 − this) to infer the high side: a known ask below 0.5
/// means the *other* (unknown) side is the high one. When neither ask is
/// known (book not yet populated at event start) falls back to `up_token`
/// (the legacy unconditional choice).
pub fn pick_probe_side<'a>(
    up_token: &'a str,
    up_ask: Option<f64>,
    down_token: &'a str,
    down_ask: Option<f64>,
) -> &'a str {
    match (up_ask, down_ask) {
        (Some(u), Some(d)) => {
            if d > u {
                down_token
            } else {
                up_token
            }
        }
        (Some(u), None) => {
            if u < 0.5 {
                down_token
            } else {
                up_token
            }
        }
        (None, Some(d)) => {
            if d < 0.5 {
                up_token
            } else {
                down_token
            }
        }
        (None, None) => up_token,
    }
}

/// Spawn the probe task on a dedicated OS thread.
///
/// Returns the JoinHandle so engine teardown can wait for it on
/// shutdown. The thread name `poly-rtt-probe-join` is intentionally
/// `*-join` so the existing OS-pinning route (`pin_background`)
/// applies — the probe is decidedly NOT latency-critical itself.
///
/// The probe always uses [`fire_full_probe`] (a real *resting* postOnly
/// place + cancel). Each leg flows through `SharedState::http_call_*`,
/// which records the per-request latency to the CSV when recording is
/// active (`latency_record`), so the probe itself does no recording.
/// Probe legs are recorded under the dedicated `probe_place` /
/// `probe_cancel` kinds (the record-replay loader folds them into the
/// place / cancel pools; offline analysis can tell them apart).
///
/// ## All-probe mode (`all_probe = true`)
///
/// Wired by the engine from `[general] all_probe` in live mode. The
/// probe ignores `enable_flag` and fires every `interval` for the whole
/// session (as long as an `active_token` is available). When
/// `all_probe = false` it behaves as the RTT-gate's latency sampler:
/// fires only while the gate is in PROBE mode (`enable_flag`).
pub fn spawn_rtt_probe(
    shared: Arc<SharedState>,
    enable_flag: Arc<AtomicBool>,
    sample_tx: Sender<f64>,
    active_token: ActiveTokenHandle,
    interval: Duration,
    shutdown: Arc<AtomicBool>,
    all_probe: bool,
    instance_id: String,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("poly-rtt-probe-join".to_string())
        .spawn(move || {
            crate::os_tune::pin_background("poly-rtt-probe-join");
            info!(
                "[RttProbe] Started (instance_id={}) — interval={:.1}s, real resting \
                 place + cancel (postOnly BUY high-side @{} size={}, never fills); \
                 all_probe={} ({}).",
                instance_id,
                interval.as_secs_f64(),
                FULL_PROBE_PRICE,
                FULL_PROBE_SIZE,
                all_probe,
                if all_probe {
                    "fires continuously"
                } else {
                    "fires only in gate PROBE mode"
                },
            );

            let poll_resolution = Duration::from_millis(100);
            let mut last_fire = Instant::now() - interval;

            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                // Normal (gate-driven) mode fires only while the gate is
                // in PROBE. All-probe mode ignores the flag — the whole
                // session is a probe session.
                if !all_probe && !enable_flag.load(Ordering::Relaxed) {
                    std::thread::sleep(poll_resolution);
                    last_fire = Instant::now() - interval;
                    continue;
                }

                if last_fire.elapsed() < interval {
                    std::thread::sleep(poll_resolution);
                    continue;
                }

                let place_rtt = fire_full_probe(&shared, &active_token, &instance_id);
                last_fire = Instant::now();
                if let Some(rtt_ms) = place_rtt {
                    debug!("[RttProbe] place RTT={:.1}ms", rtt_ms);
                    // Feed the place RTT to the gate channel (drives the
                    // RTT-gate p85). In gate-driven mode a send error means
                    // the strategy thread shut down → exit. In all-probe
                    // mode there may be NO consumer (e.g. record mode has
                    // no strategy) — the channel is best-effort there, so
                    // a disconnected send is ignored, not fatal.
                    match sample_tx.try_send(rtt_ms) {
                        Ok(()) | Err(crossbeam_channel::TrySendError::Full(_)) => {}
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) if !all_probe => {
                            break;
                        }
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
                    }
                }
            }
            info!("[RttProbe] Exiting");
        })
}

/// Probe cycle: place a real **resting** order, then cancel it. Each leg
/// goes through `SharedState::http_call_*`, which records the per-request
/// latency to the CSV when recording is active — this fn does no
/// recording itself.
///
/// The order is a `postOnly GTC BUY <high-side> @ FULL_PROBE_PRICE
/// size=FULL_PROBE_SIZE` (high side via [`pick_probe_side`]): deep enough
/// to always rest (so there is a real order to cancel) and `postOnly` so
/// it can never take a fill.
/// Both legs traverse the same auth + matching-engine paths real
/// submits / cancels hit, so the latency is faithful.
///
/// Returns `Some(place_rtt_ms)` when the place got a real round-trip
/// (for the gate channel); `None` on pre-RTT failure (no token / signing
/// / DNS / TLS / connect refused).
fn fire_full_probe(
    shared: &Arc<SharedState>,
    active_token: &ActiveTokenHandle,
    instance_id: &str,
) -> Option<f64> {
    // Live accounts always have a persistent ledger and therefore a durable
    // probe owner. CLI/test states without one skip synthetic placement.
    let orphan_owner = shared.probe_orphan_owner.as_ref()?;
    // An ambiguous prior place owns the account's only probe lease. Resolve it
    // through cancel + two independent reconcile slots; never stack another
    // synthetic order on top of unknown server state.
    match orphan_owner.current() {
        Ok(Some(orphan)) => {
            shared.probe_order_ids.insert(&orphan.order_id);
            let route = PolymarketTrade::from_shared(
                Arc::clone(shared),
                &shared.auth.api_key,
                &orphan.instance_id,
            );
            let terminal =
                match route.reconcile_probe_order(&orphan.client_order_id, &orphan.order_id) {
                    ProbeReconcileOutcome::Terminal => true,
                    ProbeReconcileOutcome::ParallelAbsent => {
                    match orphan_owner.note_parallel_absence(&orphan.order_id) {
                            Ok(observations) => observations >= 2,
                            Err(error) => {
                                warn!("[RttProbe] persist parallel evidence failed: {error}");
                                false
                            }
                        }
                    }
                    ProbeReconcileOutcome::Pending => false,
                };
            if terminal {
                match orphan_owner.resolve(&orphan.order_id) {
                    Ok(true) => info!(
                        "[RttProbe] durable orphan resolved orderID={} age_ms={:.0} probe_reserved_cash_released={:.6}",
                        orphan.order_id,
                        crate::types::now_ns().saturating_sub(orphan.created_at_ns) as f64
                            / 1_000_000.0,
                        orphan.reserved_cash_micros as f64 / 1_000_000.0,
                    ),
                    Ok(false) => {}
                    Err(error) => warn!("[RttProbe] durable orphan resolution failed: {error}"),
                }
            }
            let (high_water, overflow) = orphan_owner.metrics();
            crate::latency::record_ns(
                "polymarket.probe_orphan.queue_high_water",
                high_water as u64,
            );
            crate::latency::record_ns("polymarket.probe_orphan.queue_overflow", overflow);
            return None;
        }
        Ok(None) => {}
        Err(error) => {
            warn!("[RttProbe] orphan owner unavailable; probe remains fail-closed: {error}");
            return None;
        }
    }

    if let Some(reason) = shared.place_admission_block_reason() {
        debug!("[RttProbe] new probe blocked by {reason}");
        return None;
    }
    let token = active_token.load()?;
    if token.is_empty() {
        return None;
    }
    let signer = shared.signer_v2.as_ref()?;

    // `_dispatch`, NOT the plain `build_signed_order`: poly_1271
    // accounts need the deposit-wallet maker + ERC-7739 signature wrap,
    // exactly like a real submit. The plain path produced an unwrapped
    // EOA signature that the server rejected http_400 on EVERY probe
    // (2026-07-11..13: 122k rejects, 100% of probes on live poly_1271
    // accounts), silently degrading the probe to the reject-RTT shape
    // this module's docs warn about.
    let signed = match signer.build_signed_order_dispatch(
        &token,
        FULL_PROBE_PRICE,
        FULL_PROBE_SIZE,
        crate::types::Side::Buy,
    ) {
        Ok(s) => s,
        Err(e) => {
            warn!("[RttProbe] full-probe sign error (skip): {}", e);
            return None;
        }
    };
    let salt_u64: u64 = signed
        .order
        .salt
        .parse::<u128>()
        .map(|v| v as u64)
        .unwrap_or(0);
    let probe_coid = format!("probe:{}:{}", instance_id, signed.order_hash);
    if shared.account_state.is_seeded()
        && shared.account_state.monitoring_snapshot().unallocated_cash
            < FULL_PROBE_PRICE * FULL_PROBE_SIZE * 2.0
    {
        debug!("[RttProbe] insufficient operational cash slack; probe skipped");
        return None;
    }

    // Wire body mirrors `sign_and_build_body_v2`, but `postOnly: true`
    // so the resting order can never accidentally take a fill.
    let body = serde_json::json!({
        "owner": shared.auth.api_key,
        "orderType": "GTC",
        "postOnly": true,
        "deferExec": false,
        "order": {
            "salt": salt_u64,
            "maker": signed.order.maker,
            "signer": signed.order.signer,
            "taker": signed.order.taker,
            "tokenId": signed.order.token_id,
            "makerAmount": signed.order.maker_amount,
            "takerAmount": signed.order.taker_amount,
            "side": "BUY",
            "signatureType": signed.order.signature_type,
            "timestamp": signed.order.timestamp,
            "expiration": signed.order.expiration,
            "metadata": signed.order.metadata,
            "builder": signed.order.builder,
            "signature": signed.signature,
        }
    })
    .to_string();

    let orphan = ProbeOrphan {
        instance_id: instance_id.to_string(),
        client_order_id: probe_coid.clone(),
        order_id: signed.order_hash.clone(),
        token_id: token.to_string(),
        created_at_ns: crate::types::now_ns(),
        reserved_cash_micros: (FULL_PROBE_PRICE * FULL_PROBE_SIZE * 1_000_000.0).round() as u64,
        parallel_absence_observations: 0,
    };
    match orphan_owner.acquire(orphan) {
        Ok(true) => {}
        Ok(false) => return None,
        Err(error) => {
            warn!("[RttProbe] durable orphan lease failed; probe skipped: {error}");
            return None;
        }
    }

    // Register the probe's orderID (== local order hash) BEFORE sending
    // so the user feed can identify the resting order's placement /
    // cancellation pushes as probe traffic (mute + don't forward) even
    // when the push races ahead of the place response.
    shared.probe_order_ids.insert(&signed.order_hash);

    // ── Place leg ──────────────────────────────────────────────────
    // The http layer records this request's latency when active, under
    // the dedicated `probe_place` kind (not `place`) so offline analysis
    // can separate synthetic probe traffic from real strategy orders.
    let t0 = Instant::now();
    let res = shared.http_call_sync_rec(
        "POST",
        "/order",
        &body,
        Some(crate::latency_record::RequestKind::ProbePlace),
    );
    let place_rtt = t0.elapsed().as_secs_f64() * 1000.0;

    // Resolve the resting order's id for the cancel leg. The server's
    // `orderID` (when the place is accepted) is authoritative; it equals
    // the locally-computed EIP-712 `order_hash`, which we fall back to.
    let (order_id, place_round_trip): (Option<String>, bool) = match &res {
        Ok(json) => {
            let oid = json
                .get("orderID")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| signed.order_hash.clone());
            (Some(oid), true)
        }
        Err(HttpErr::Timeout) | Err(HttpErr::Transport(_)) | Err(HttpErr::InvalidResponse(_)) => {
            // The order's fate is unknown: the request may have rested
            // server-side and only the response was lost. Best-effort
            // cancel via the locally-computed order_hash (== Polymarket
            // orderID) so a degraded session can't accrue orphaned resting
            // collateral. This includes timeout/transport/malformed replies
            // and server deadline failures whose acceptance state is not
            // authoritative.
            (Some(signed.order_hash.clone()), true)
        }
        Err(error @ HttpErr::Status(_, _)) if error.is_submit_unknown_state() => {
            // Some server deadline/status replies are explicitly classified
            // as submit-unknown by the normal order path as well.
            (Some(signed.order_hash.clone()), true)
        }
        Err(HttpErr::Status(code, body)) => {
            // Real round-trip but the server rejected it (e.g. balance /
            // tick / min-size / bad signature) — there's no resting order
            // to cancel. Rejection is NOT a healthy probe outcome: warn
            // (rate-limited) with the reason.
            warn_probe_place_rejected(*code, body);
            let _ = orphan_owner.resolve(&signed.order_hash);
            (None, true)
        }
        Err(e @ HttpErr::Other(_)) => {
            warn!("[RttProbe] probe place transport error (skip): {:?}", e);
            let _ = orphan_owner.resolve(&signed.order_hash);
            (None, false)
        }
    };

    if !place_round_trip {
        return None;
    }

    // ── Cancel leg ─────────────────────────────────────────────────
    // Only when the place produced a (presumed) resting order. Latency
    // is recorded at the http layer; we just fire it and log.
    if let Some(oid) = order_id {
        let cbody = serde_json::json!({ "orderID": oid }).to_string();
        let cres = shared.http_call_sync_rec(
            "DELETE",
            "/order",
            &cbody,
            Some(crate::latency_record::RequestKind::ProbeCancel),
        );
        if cres
            .as_ref()
            .is_ok_and(|response| probe_cancel_response_is_terminal(response, &oid))
        {
            let _ = orphan_owner.resolve(&oid);
        }
        debug!(
            "[RttProbe] probe place={:.1}ms cancel_ok={}",
            place_rtt,
            cres.is_ok(),
        );
    }

    Some(place_rtt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_token_handle_publishes_latest_immutable_snapshot() {
        let writer = ActiveTokenHandle::new(None);
        let reader = writer.clone();
        assert!(reader.load().is_none());

        writer.store(Some("UP".to_string()));
        let retained = reader.load().unwrap();
        assert_eq!(retained.as_str(), "UP");

        writer.store(Some("DOWN".to_string()));
        assert_eq!(retained.as_str(), "UP", "published snapshots are immutable");
        assert_eq!(reader.load().unwrap().as_str(), "DOWN");

        writer.store(None);
        assert!(reader.load().is_none());
    }

    #[test]
    fn probe_orphan_owner_is_ordered_idempotent_and_restart_durable() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = dir.path().join("zhu02.json");
        let shutdown = hexagent_runtime::shutdown::ShutdownToken::new();
        let (owner, join) = spawn_probe_orphan_owner(Some(&ledger), shutdown.clone()).unwrap();
        let orphan = ProbeOrphan {
            instance_id: "btc01".into(),
            client_order_id: "probe:btc01:abc".into(),
            order_id: "0xabc".into(),
            token_id: "token-a".into(),
            created_at_ns: 7,
            reserved_cash_micros: 1_000_000,
            parallel_absence_observations: 0,
        };
        assert!(owner.acquire(orphan.clone()).unwrap());
        assert!(!owner.acquire(orphan.clone()).unwrap());
        assert_eq!(owner.current().unwrap(), Some(orphan.clone()));
        assert_eq!(owner.note_parallel_absence("0xabc").unwrap(), 1);
        shutdown.finish();
        join.join().unwrap();

        let restart = hexagent_runtime::shutdown::ShutdownToken::new();
        let (owner, join) = spawn_probe_orphan_owner(Some(&ledger), restart.clone()).unwrap();
        assert_eq!(
            owner
                .current()
                .unwrap()
                .unwrap()
                .parallel_absence_observations,
            1
        );
        assert!(!owner.resolve("different").unwrap());
        assert!(owner.resolve("0xABC").unwrap());
        assert!(!owner.resolve("0xabc").unwrap());
        assert!(owner.current().unwrap().is_none());
        restart.finish();
        join.join().unwrap();
    }
}
