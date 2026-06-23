//! RTT-probe task — synthetic latency measurement via real signed orders.
//!
//! Live partner of `strategy::polymaker::rtt_gate`. While the strategy
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
//! `Arc<Mutex<Option<String>>>` and refreshes it as the book moves,
//! clearing it on settlement. When `None` (no active event in this
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
//! * **Timeout** (HttpErr::Timeout, h2/connect timeout) — recorded
//!   with the elapsed time as the sample. Timeout IS the primary
//!   failure mode the gate exists to detect; suppressing it would
//!   blind the gate to network degradation.
//! * Pre-RTT failures (DNS, TLS handshake, connection refused —
//!   HttpErr::Other) — skipped. Those happen before any round trip
//!   and aren't representative of submit latency.
//!
//! Per-call timeouts are bounded by the FAST h2 client pool ceiling
//! (typically 1500–2000 ms via `async_rt::current_fast_timeout`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use log::{debug, info, warn};

use super::trade::{HttpErr, SharedState};

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

/// Strategy → probe handoff for the current event's probe-target token
/// (the high-priced binary side; see [`pick_probe_side`]). `Some(token)`
/// when an event is active in the polymaker series; `None` otherwise.
/// Probe reads on every place cycle; the writer (strategy or recorder)
/// sets it at event start and refreshes it as the book moves, clearing
/// it on settlement.
pub type ActiveTokenHandle = Arc<Mutex<Option<String>>>;

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
        (Some(u), Some(d)) => if d > u { down_token } else { up_token },
        (Some(u), None) => if u < 0.5 { down_token } else { up_token },
        (None, Some(d)) => if d < 0.5 { up_token } else { down_token },
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
                instance_id, interval.as_secs_f64(), FULL_PROBE_PRICE, FULL_PROBE_SIZE,
                all_probe,
                if all_probe { "fires continuously" } else { "fires only in gate PROBE mode" },
            );

            let poll_resolution = Duration::from_millis(100);
            let mut last_fire = Instant::now() - interval;

            loop {
                if shutdown.load(Ordering::Relaxed) { break; }

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

                let place_rtt = fire_full_probe(&shared, &active_token);
                last_fire = Instant::now();
                if let Some(rtt_ms) = place_rtt {
                    debug!("[RttProbe] place RTT={:.1}ms", rtt_ms);
                    // Feed the place RTT to the gate channel (drives the
                    // RTT-gate p85). In gate-driven mode a send error means
                    // the strategy thread shut down → exit. In all-probe
                    // mode there may be NO consumer (e.g. record mode has
                    // no strategy) — the channel is best-effort there, so
                    // a disconnected send is ignored, not fatal.
                    if sample_tx.send(rtt_ms).is_err() && !all_probe {
                        break;
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
) -> Option<f64> {
    let token = active_token.lock().ok()?.clone()?;
    if token.is_empty() { return None; }
    let signer = shared.signer_v2.as_ref()?;

    let signed = match signer.build_signed_order(
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
    let salt_u64: u64 = signed.order.salt.parse::<u128>()
        .map(|v| v as u64).unwrap_or(0);

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
    }).to_string();

    // ── Place leg ──────────────────────────────────────────────────
    // (the http layer records this request's latency when active)
    let t0 = Instant::now();
    let res = shared.http_call_sync("POST", "/order", &body);
    let place_rtt = t0.elapsed().as_secs_f64() * 1000.0;

    // Resolve the resting order's id for the cancel leg. The server's
    // `orderID` (when the place is accepted) is authoritative; it equals
    // the locally-computed EIP-712 `order_hash`, which we fall back to.
    let (order_id, place_round_trip): (Option<String>, bool) = match &res {
        Ok(json) => {
            let oid = json.get("orderID")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .unwrap_or_else(|| signed.order_hash.clone());
            (Some(oid), true)
        }
        Err(HttpErr::Timeout) => {
            // The order's fate is unknown: the request may have rested
            // server-side and only the response was lost. Best-effort
            // cancel via the locally-computed order_hash (== Polymarket
            // orderID) so a degraded session can't accrue orphaned
            // resting collateral. A truly-failed place just 404s the
            // cancel (recorded with status `http_404`, filterable).
            (Some(signed.order_hash.clone()), true)
        }
        Err(HttpErr::Status(_, _)) => {
            // Real round-trip but the server rejected it (e.g. balance /
            // tick / min-size) — there's no resting order to cancel.
            (None, true)
        }
        Err(e @ HttpErr::Other(_)) => {
            warn!("[RttProbe] probe place transport error (skip): {:?}", e);
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
        let cres = shared.http_call_sync("DELETE", "/order", &cbody);
        debug!(
            "[RttProbe] probe place={:.1}ms cancel_ok={}",
            place_rtt, cres.is_ok(),
        );
    }

    Some(place_rtt)
}

// Silence unused-warning for Mutex on platforms that re-export it
// only when active_token is constructed by the engine.
#[allow(dead_code)]
fn _mutex_keep_in_scope(_: &Mutex<Option<String>>) {}
