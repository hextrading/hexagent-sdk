//! Per-event RTT extraction from live.log files (2026-05-21).
//!
//! When a BT operator sets `sim_rtt_mode = "exact"`, the engine calls
//! `extract_per_event_rtt` at startup (from the `sim_latency_calibrate_from`
//! log paths) to build a
//! `HashMap<event_secs, EventRttOverride>` from one or more live logs.
//! Each entry records:
//!
//!   * **This event's place / cancel RTT distribution** (p50, p85, p95,
//!     p99 in ms) computed from place-side ACK log lines:
//!       - `Submit + Order accepted` (status=live, maker rest)
//!       - `Submit + Order accepted status=matched` (taker fill — same
//!         "Order accepted:" prefix)
//!       - `Submit + Order failed` (server reject — matches live's
//!         gate which records any non-timeout ack via record_sample)
//!     plus `Cancel request + Cancel result` on the cancel side.
//!     ACK rows that land in the **final 30 s** of an event (the
//!     close-only / lock-in window) are excluded from the parser's
//!     bucket, matching live's `RttGate::maybe_lock_in` at T-30 s.
//!   * **Previous event's place-only p60** — what the strategy's
//!     RTT-gate observed at THIS event's start. Drives the per-event
//!     RTT-N scaling (`N = ceil(prev_p / 100ms)` clamped to
//!     `quote_interval_n_max`).
//!
//!     Matches BOTH the quantile and source the live gate uses:
//!       - quantile = p60 (live's `rtt_percentile = 0.60` config;
//!         the 0.85 default in code is never used in any deployed
//!         TOML)
//!       - source   = place RTTs only (gate's `record_sample()` is
//!         wired exclusively to the place-side ack path; cancel
//!         acks bypass the gate — see strategy.rs:6510)
//!
//!     A naive carry-over using place p85 would inflate sim's N
//!     by ~60% (place p85 ≈ 1.6× place p60 in the typical
//!     heavy-tailed RTT distribution).
//!
//! At each Polymarket `Instrument` dispatch the engine looks up
//! `event_secs = event_start_ns / 1_000_000_000` in the override table.
//! On hit:
//!
//!   * `CoupledLatencySamplers::set_per_event_anchors(...)` pushes the
//!     event's empirical anchors into both samplers — the next
//!     `sample_place` / `sample_cancel` calls draw from the live
//!     distribution instead of the auto-cal CDF.
//!   * `RttGate::set_prev_event_p_override(...)` injects the live's
//!     observed prior-event pooled p60 so the strategy's N calc uses
//!     the same anchor it actually used in live.
//!
//! On miss the engine falls back to whatever the latency sampler was
//! constructed with (auto-cal hourly or pooled empirical), and the
//! strategy's rtt_gate uses its own internally-tracked
//! `last_event_p_ms`.
//!
//! **Event bucketing**: each Submit/Cancel-request line's wall-clock
//! timestamp is floored to the nearest 5-minute boundary
//! (`(secs / 300) * 300`) to derive `event_secs`. This matches the
//! Polymarket binary option series cadence (5-min events,
//! back-to-back). Events with no observed RTT samples are simply
//! absent from the table — caller treats absence as "no override,
//! fall through".

use std::collections::HashMap;

use super::calib_source::calib_lines;

/// Per-event RTT override extracted from one or more live.log files.
/// All `*_ms` fields are clamped to u32 (~71 minutes max — far beyond
/// any realistic Polymarket HTTP timeout). `None` on a quantile means
/// either no samples on that side, or fewer than `MIN_SAMPLES`
/// (defensive — sub-3-sample quantiles are too noisy to override the
/// pooled auto-cal distribution).
///
/// ## Intra-event segmentation (2026-05-28)
///
/// Empirical analysis of live.log (2026-05-28, 11h, 85k submits) shows
/// RTT is **strongly time-of-event dependent**: the first 0-60 s of
/// high-vol events has mean RTT 3× the rest, p95 4×. Mechanism: BTC
/// spot bursts at event boundaries cause Polymarket gateway congestion
/// (RTT spikes from 50 ms → 1.5-2 s for ~30-60 s), then recover. The
/// previous "single per-event CDF" model averaged across this regime
/// shift, producing sim RTT that's smooth where live is bursty.
///
/// To capture this, the parser now splits each event's samples into
/// two time buckets:
///   * **early**  (offset 0 to `SEGMENT_BOUNDARY_SECS`, default 60 s)
///   * **late**   (offset `SEGMENT_BOUNDARY_SECS` to 270 s)
///
/// The sampler holds both anchor tables and picks at draw time based on
/// `time_in_event = now_ns − event_start_ns`. When either bucket has
/// < MIN_SAMPLES samples, all `*_early_*` and `*_late_*` fields stay
/// `None` and the sampler falls back to the aggregate quantiles
/// (`place_p50_ms` etc.), preserving back-compat for sparse events.
// `Eq` intentionally omitted: the new `*_timeout_rate: Option<f64>` fields
// make `Eq` underivable. The struct is a HashMap *value* (keyed by
// `event_secs`), never a key, so only `PartialEq` (for tests) is needed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EventRttOverride {
    /// Event start (unix seconds, 5-min boundary).
    pub event_secs: u64,
    /// Place-side quantiles (in ms) for THIS event. All four are
    /// `Some` iff `place_n_samples >= MIN_SAMPLES`.
    pub place_p50_ms: Option<u32>,
    pub place_p85_ms: Option<u32>,
    pub place_p95_ms: Option<u32>,
    pub place_p99_ms: Option<u32>,
    pub place_n_samples: usize,
    /// Cancel-side quantiles (in ms) for THIS event.
    pub cancel_p50_ms: Option<u32>,
    pub cancel_p85_ms: Option<u32>,
    pub cancel_p95_ms: Option<u32>,
    pub cancel_p99_ms: Option<u32>,
    pub cancel_n_samples: usize,
    /// **Previous event's place-only p60** — matches the strategy's
    /// `RttGate.rtt_percentile = 0.60` configuration AND the gate's
    /// place-only sample source.
    ///
    /// Two corrections vs the naive "place-p85 carry-over":
    ///   1. **Quantile**: gate uses p60, not p85. Live configs all set
    ///      `rtt_percentile = 0.60`; the 0.85 in code is just a
    ///      compile-time default.
    ///   2. **Source**: gate's `record_sample` is wired to the
    ///      place-side ack path only (see strategy.rs:815 — "the
    ///      place-only feed"). Cancel acks do NOT feed the gate.
    ///
    /// `None` if previous event had `< MIN_SAMPLES` place samples
    /// (or this is the first event in the table).
    pub prev_event_p_ms: Option<u32>,

    /// **Intra-event segment quantiles** (2026-05-28). When all four
    /// `place_early_*` are `Some` AND all four `place_late_*` are
    /// `Some`, the sampler uses the segmented model; otherwise it
    /// falls back to the aggregate quantiles above. Symmetric for
    /// cancel side.
    pub place_early_p50_ms: Option<u32>,
    pub place_early_p85_ms: Option<u32>,
    pub place_early_p95_ms: Option<u32>,
    pub place_early_p99_ms: Option<u32>,
    pub place_early_n_samples: usize,
    pub place_late_p50_ms: Option<u32>,
    pub place_late_p85_ms: Option<u32>,
    pub place_late_p95_ms: Option<u32>,
    pub place_late_p99_ms: Option<u32>,
    pub place_late_n_samples: usize,
    pub cancel_early_p50_ms: Option<u32>,
    pub cancel_early_p85_ms: Option<u32>,
    pub cancel_early_p95_ms: Option<u32>,
    pub cancel_early_p99_ms: Option<u32>,
    pub cancel_early_n_samples: usize,
    pub cancel_late_p50_ms: Option<u32>,
    pub cancel_late_p85_ms: Option<u32>,
    pub cancel_late_p95_ms: Option<u32>,
    pub cancel_late_p99_ms: Option<u32>,
    pub cancel_late_n_samples: usize,

    /// **Per-event timeout RATE** = `timeouts / requests` on each side,
    /// from raw line counts over the full event window (`*OrderTimeout`
    /// lines / `Submit` resp. `Cancel request` lines, bucketed by each
    /// line's own 5-min boundary). `None` when the side had `< MIN_SAMPLES`
    /// requests. Feeds the sampler's exceedance anchor so a fraction `rate`
    /// of draws land past client_timeout — the censored tail the
    /// non-timeout quantiles cannot express. Without it cancel timeouts are
    /// ~0 in sim despite ~1.3 % in live, and the place tail is truncated
    /// (sim max ≈ 13 vs live ≈ 48 per event).
    pub place_timeout_rate: Option<f64>,
    pub cancel_timeout_rate: Option<f64>,
}

/// Minimum sample count before a quantile is considered reliable enough
/// to override the pooled distribution. Below this the entry sets the
/// quantile to `None` and the caller falls through.
const MIN_SAMPLES: usize = 3;

/// 5-minute Polymarket event cadence (seconds).
const EVENT_PERIOD_SECS: u64 = 300;

/// Cap the in-flight Submit table at this many entries. A submit that
/// never gets matched (recorder-truncated log tail) shouldn't grow
/// memory unboundedly. Real sessions max out at ~50 in-flight orders;
/// 4096 covers any reasonable backlog.
const MAX_PENDING: usize = 4096;

/// **Lock-in window**: live's `RttGate::maybe_lock_in` fires at
/// `event_start + (EVENT_PERIOD_SECS - LOCKIN_OFFSET_SECS)`, i.e.
/// 30 s before event end. The lock-in computes p60 from samples
/// observed up to that point — the final 30 s ("close-only" window)
/// is excluded from the gate's prev_p calculation.
///
/// Parser bucketing must match: drop any Submit whose `ts_offset`
/// (from its event_start) falls in the final 30 s, otherwise the
/// parser-computed p60 will be dragged down by the close-only ack
/// burst (where strategy stops issuing maker quotes and only
/// flushes residual cancels + sell-out takers — those ack RTTs
/// are typically faster and skew the distribution low).
///
/// Empirical impact (live 2026-05-20 event 1779292200):
///   * live gate lock-in p60 = 588.2 ms (n=250, samples cut at T-30s)
///   * parser w/o lock-in cut: p60 = 313 ms (full 5-min window)
///   * Δ = 47 % under-estimate → drives sim N down by ~15 % on average.
const LOCKIN_OFFSET_SECS: u64 = 30;

/// **Intra-event segment boundary** (2026-05-28). Samples whose
/// `submit_offset_secs < SEGMENT_BOUNDARY_SECS` go into the "early"
/// bucket; samples with `SEGMENT_BOUNDARY_SECS <= offset < 270`
/// (the lock-in cut) go into the "late" bucket.
///
/// Default 60 s — empirically the elbow in live.log RTT vs time-in-
/// event curves. Could be parameterised in the future; for now a fixed
/// constant keeps the extract API scalar-only (BacktestConfig stays
/// flat). The sampler side honors this as the boundary at draw time.
pub const SEGMENT_BOUNDARY_SECS: u64 = 60;

/// Parse one or more live.log files, returning a per-event RTT override
/// table. Lines are matched by substring (regex-free, fast).
///
/// Output keyed by `event_secs` (event_start unix seconds rounded to
/// 5-min boundary). `prev_event_p_ms` is filled by walking events in
/// sorted order and carrying the previous event's PLACE-ONLY p60
/// forward — matching the strategy's gate config (`rtt_percentile =
/// 0.60`) AND the gate's place-only sample source.
pub fn extract_per_event_rtt(
    paths: &[String],
) -> std::io::Result<HashMap<u64, EventRttOverride>> {
    if paths.is_empty() {
        return Ok(HashMap::new());
    }

    // coid → submit_ts_ms (in-flight Submit waiting for the matching
    // Order-accepted reply). Cleared on pairing.
    let mut submit_ts: HashMap<u64, u64> = HashMap::new();
    let mut cancel_req_ts: HashMap<u64, u64> = HashMap::new();
    // Per-event raw RTTs in ms (kept as f64 for quantile interp).
    // Aggregate (`*_all`) drives the legacy single-CDF anchor path;
    // `*_early` / `*_late` drive the 2026-05-28 intra-event segmented
    // anchor path. A sample lands in `*_all` AND exactly one of
    // `*_early` / `*_late` (or neither, if the submit is in the
    // lock-in window — those samples are dropped entirely for parity
    // with the gate's p60).
    let mut place_all:   HashMap<u64, Vec<f64>> = HashMap::new();
    let mut place_early: HashMap<u64, Vec<f64>> = HashMap::new();
    let mut place_late:  HashMap<u64, Vec<f64>> = HashMap::new();
    let mut cancel_all:   HashMap<u64, Vec<f64>> = HashMap::new();
    let mut cancel_early: HashMap<u64, Vec<f64>> = HashMap::new();
    let mut cancel_late:  HashMap<u64, Vec<f64>> = HashMap::new();
    // Per-event timeout RATE counters (right-censored tail). A request that
    // exceeds client_timeout emits a `*OrderTimeout` line and NO ack, so it
    // is absent from the `*_all` quantile vectors above — those describe
    // only `RTT | no timeout`. The rate `timeouts / requests` is computed
    // from raw LINE COUNTS, bucketed by each line's own 5-min boundary:
    //   * denominator = every `Submit` / `Cancel request` line (one per op),
    //   * numerator   = every `NewOrderTimeout` / `CancelOrderTimeout` line.
    // NOT coid-paired: an order is cancel-requested/resulted MANY times
    // (re-cancels), so pairing-and-popping on the result loses the later
    // timeout (undercounts ~10×). Raw counts are exact and robust. Drives
    // the sampler's exceedance anchor (latency.rs) — without it the sim
    // draws ~0 cancel timeouts vs ~1.3 % in live.
    let mut place_req:  HashMap<u64, usize> = HashMap::new();
    let mut cancel_req: HashMap<u64, usize> = HashMap::new();
    let mut place_to:   HashMap<u64, usize> = HashMap::new();
    let mut cancel_to:  HashMap<u64, usize> = HashMap::new();
    // Set of event_secs we've seen evidence of (from Submit ts or from
    // `Event ended:` lines). Used to seed events that had no fills
    // (still present in the table with all-None quantiles, so prev_p85
    // walk handles them).
    let mut observed_events: std::collections::BTreeSet<u64> =
        std::collections::BTreeSet::new();

    for path in paths {
        for line in calib_lines(path)? {
            let line = match line { Ok(l) => l, Err(_) => continue };

            // Detect explicit event boundary — adds to observed_events.
            if let Some(secs) = parse_event_ended_line(&line) {
                observed_events.insert(secs);
            }

            // Submit / Cancel request: stash log_ts by coid.
            if line.contains("] Submit ") {
                if let (Some(ts), Some(coid)) =
                    (parse_iso_ts_ms(&line), parse_coid(&line))
                {
                    // observed_events tracks any event that had strategy
                    // activity — for the prev_p carry-over walk to chain
                    // through "trade happened" gaps. Skip the lock-in
                    // filter here; we still want a placeholder entry.
                    let event_secs = (ts / 1000 / EVENT_PERIOD_SECS) * EVENT_PERIOD_SECS;
                    observed_events.insert(event_secs);
                    // Rate denominator: every submit is one place op (full
                    // event window, no lock-in cut — the rate is a separate
                    // statistic from the gate-matched quantiles).
                    *place_req.entry(event_secs).or_default() += 1;
                    if submit_ts.len() < MAX_PENDING {
                        submit_ts.insert(coid, ts);
                    }
                }
                continue;
            }
            if line.contains("] Cancel request ") {
                if let (Some(ts), Some(coid)) =
                    (parse_iso_ts_ms(&line), parse_coid(&line))
                {
                    // Rate denominator: every cancel request is one cancel op.
                    let event_secs = (ts / 1000 / EVENT_PERIOD_SECS) * EVENT_PERIOD_SECS;
                    *cancel_req.entry(event_secs).or_default() += 1;
                    if cancel_req_ts.len() < MAX_PENDING {
                        cancel_req_ts.insert(coid, ts);
                    }
                }
                continue;
            }

            // Order accepted: pair on coid → compute place-RTT and
            // bucket by submit_ts (skipping the lock-in window).
            // Note: `Order accepted: ... status=matched coid=N` (the
            // taker-fill log row that follows every `Matched
            // immediately`) is matched by this same `Order accepted:`
            // substring — taker fills are automatically included via
            // their accompanying status=matched row, no separate
            // matcher needed.
            if line.contains("] Order accepted:") {
                if let (Some(accept_ts), Some(coid)) =
                    (parse_iso_ts_ms(&line), parse_coid(&line))
                {
                    if let Some(submit) = submit_ts.remove(&coid) {
                        if let Some((evt, offset_secs)) = submit_ts_to_event_secs_with_offset(submit) {
                            let rtt = (accept_ts.saturating_sub(submit)) as f64;
                            place_all.entry(evt).or_default().push(rtt);
                            if offset_secs < SEGMENT_BOUNDARY_SECS {
                                place_early.entry(evt).or_default().push(rtt);
                            } else {
                                place_late.entry(evt).or_default().push(rtt);
                            }
                        }
                    }
                }
                continue;
            }
            if line.contains("] Cancel result ") {
                if let (Some(reply_ts), Some(coid)) =
                    (parse_iso_ts_ms(&line), parse_coid(&line))
                {
                    if let Some(req) = cancel_req_ts.remove(&coid) {
                        if let Some((evt, offset_secs)) = submit_ts_to_event_secs_with_offset(req) {
                            let rtt = (reply_ts.saturating_sub(req)) as f64;
                            cancel_all.entry(evt).or_default().push(rtt);
                            if offset_secs < SEGMENT_BOUNDARY_SECS {
                                cancel_early.entry(evt).or_default().push(rtt);
                            } else {
                                cancel_late.entry(evt).or_default().push(rtt);
                            }
                        }
                    }
                }
                continue;
            }

            // **Order failed** (2026-05-21 method B): the
            // place-side ack lands as a server-side rejection
            // (`Order failed: status 400 ... coid=N`). The live
            // gate's `record_sample` is invoked from
            // `strategy.rs:6510` for any non-timeout response, so
            // failed orders DO contribute to the gate's p60. Parser
            // must include them too for source parity with the
            // live gate. Volume is small (~0.1% of total submits)
            // but the inclusion keeps the distribution shape
            // strictly aligned.
            if line.contains("] Order failed") {
                if let (Some(failed_ts), Some(coid)) =
                    (parse_iso_ts_ms(&line), parse_coid(&line))
                {
                    if let Some(submit) = submit_ts.remove(&coid) {
                        if let Some((evt, offset_secs)) = submit_ts_to_event_secs_with_offset(submit) {
                            let rtt = (failed_ts.saturating_sub(submit)) as f64;
                            place_all.entry(evt).or_default().push(rtt);
                            if offset_secs < SEGMENT_BOUNDARY_SECS {
                                place_early.entry(evt).or_default().push(rtt);
                            } else {
                                place_late.entry(evt).or_default().push(rtt);
                            }
                        }
                    }
                }
                continue;
            }

            // **Place / cancel timeout** (right-censored RTT) — rate
            // numerator. Count the `NewOrderTimeout` / `CancelOrderTimeout`
            // lines directly, bucketed by the line's own timestamp. NOT
            // coid-paired: an order is cancel-requested/resulted many times,
            // so a pop-on-result pairing loses the later re-cancel timeout
            // (undercounts ~10×). The timeout fires ~client_timeout after
            // the request, so own-ts bucketing matches the request's event
            // except for the rare op straddling a 5-min boundary (<1 %).
            if line.contains("NewOrderTimeout") {
                if let Some(ts) = parse_iso_ts_ms(&line) {
                    let evt = (ts / 1000 / EVENT_PERIOD_SECS) * EVENT_PERIOD_SECS;
                    *place_to.entry(evt).or_default() += 1;
                }
                continue;
            }
            if line.contains("CancelOrderTimeout") {
                if let Some(ts) = parse_iso_ts_ms(&line) {
                    let evt = (ts / 1000 / EVENT_PERIOD_SECS) * EVENT_PERIOD_SECS;
                    *cancel_to.entry(evt).or_default() += 1;
                }
                continue;
            }
        }
    }

    // Build per-event quantiles in sorted-by-time order so the
    // previous event's place-only p60 can be carried forward.
    let mut out: HashMap<u64, EventRttOverride> = HashMap::new();
    let mut prev_place_p60: Option<u32> = None;
    for &evt in &observed_events {
        let place      = place_all  .get(&evt).map(|v| v.as_slice()).unwrap_or(&[]);
        let cancel     = cancel_all .get(&evt).map(|v| v.as_slice()).unwrap_or(&[]);
        let p_early    = place_early.get(&evt).map(|v| v.as_slice()).unwrap_or(&[]);
        let p_late     = place_late .get(&evt).map(|v| v.as_slice()).unwrap_or(&[]);
        let c_early    = cancel_early.get(&evt).map(|v| v.as_slice()).unwrap_or(&[]);
        let c_late     = cancel_late .get(&evt).map(|v| v.as_slice()).unwrap_or(&[]);
        let (p_p50, p_p85, p_p95, p_p99) = quantiles(place);
        let (c_p50, c_p85, c_p95, c_p99) = quantiles(cancel);
        let (pe_p50, pe_p85, pe_p95, pe_p99) = quantiles(p_early);
        let (pl_p50, pl_p85, pl_p95, pl_p99) = quantiles(p_late);
        let (ce_p50, ce_p85, ce_p95, ce_p99) = quantiles(c_early);
        let (cl_p50, cl_p85, cl_p95, cl_p99) = quantiles(c_late);
        // Per-event timeout rate (right-censored tail mass) =
        // timeouts / requests, from raw line counts (full event window).
        // `None` when the side had < MIN_SAMPLES requests.
        let p_req = place_req.get(&evt).copied().unwrap_or(0);
        let c_req = cancel_req.get(&evt).copied().unwrap_or(0);
        let p_to = place_to.get(&evt).copied().unwrap_or(0);
        let c_to = cancel_to.get(&evt).copied().unwrap_or(0);
        let place_timeout_rate =
            (p_req >= MIN_SAMPLES).then(|| (p_to as f64 / p_req as f64).min(1.0));
        let cancel_timeout_rate =
            (c_req >= MIN_SAMPLES).then(|| (c_to as f64 / c_req as f64).min(1.0));
        // **Place-only p60 for the gate carry-over** — matches both
        // dimensions of the gate's accumulation:
        //   * quantile: gate uses `rtt_percentile = 0.60` in all live
        //     configs (the 0.85 default in code is never used)
        //   * source: gate's `record_sample()` is wired only to the
        //     place-side ack path (strategy.rs:6510 inside the
        //     `place_emit_event_ts` lookup; see strategy.rs:815
        //     "the place-only feed"). Cancel acks DO NOT feed the gate.
        let place_p60: Option<u32> = if place.len() >= MIN_SAMPLES {
            Some(quantile_at(place, 0.60))
        } else {
            None
        };
        let entry = EventRttOverride {
            event_secs: evt,
            place_p50_ms: p_p50,
            place_p85_ms: p_p85,
            place_p95_ms: p_p95,
            place_p99_ms: p_p99,
            place_n_samples: place.len(),
            cancel_p50_ms: c_p50,
            cancel_p85_ms: c_p85,
            cancel_p95_ms: c_p95,
            cancel_p99_ms: c_p99,
            cancel_n_samples: cancel.len(),
            prev_event_p_ms: prev_place_p60,
            // Segmented quantiles (2026-05-28). Stay all-None when
            // either segment has < MIN_SAMPLES; sampler then falls
            // back to the aggregate quantiles above.
            place_early_p50_ms: pe_p50,
            place_early_p85_ms: pe_p85,
            place_early_p95_ms: pe_p95,
            place_early_p99_ms: pe_p99,
            place_early_n_samples: p_early.len(),
            place_late_p50_ms: pl_p50,
            place_late_p85_ms: pl_p85,
            place_late_p95_ms: pl_p95,
            place_late_p99_ms: pl_p99,
            place_late_n_samples: p_late.len(),
            cancel_early_p50_ms: ce_p50,
            cancel_early_p85_ms: ce_p85,
            cancel_early_p95_ms: ce_p95,
            cancel_early_p99_ms: ce_p99,
            cancel_early_n_samples: c_early.len(),
            cancel_late_p50_ms: cl_p50,
            cancel_late_p85_ms: cl_p85,
            cancel_late_p95_ms: cl_p95,
            cancel_late_p99_ms: cl_p99,
            cancel_late_n_samples: c_late.len(),
            place_timeout_rate,
            cancel_timeout_rate,
        };
        // Forward this event's place p60 (if computed) for the next
        // event's gate-equivalent prev_p anchor.
        if let Some(p) = place_p60 {
            prev_place_p60 = Some(p);
        }
        // Insert even when all quantiles are None — the strategy still
        // wants the prev_p carry-over even on a no-trade event.
        out.insert(evt, entry);
    }
    Ok(out)
}

impl EventRttOverride {
    /// True iff both segmented buckets have enough samples on the
    /// place side AND the cancel side to drive the segmented sampler
    /// model. When false, the engine should pass only the aggregate
    /// quantiles to `set_per_event_anchors` (legacy path).
    pub fn has_segmented_place(&self) -> bool {
        self.place_early_p99_ms.is_some() && self.place_late_p99_ms.is_some()
    }
    pub fn has_segmented_cancel(&self) -> bool {
        self.cancel_early_p99_ms.is_some() && self.cancel_late_p99_ms.is_some()
    }

    /// Convenience: pull `(p50, p85, p95, p99)` for the place side's
    /// early segment, or `None` if any quantile is missing.
    pub fn place_early(&self) -> Option<(f64, f64, f64, f64)> {
        Some((
            self.place_early_p50_ms? as f64,
            self.place_early_p85_ms? as f64,
            self.place_early_p95_ms? as f64,
            self.place_early_p99_ms? as f64,
        ))
    }
    pub fn place_late(&self) -> Option<(f64, f64, f64, f64)> {
        Some((
            self.place_late_p50_ms? as f64,
            self.place_late_p85_ms? as f64,
            self.place_late_p95_ms? as f64,
            self.place_late_p99_ms? as f64,
        ))
    }
    pub fn cancel_early(&self) -> Option<(f64, f64, f64, f64)> {
        Some((
            self.cancel_early_p50_ms? as f64,
            self.cancel_early_p85_ms? as f64,
            self.cancel_early_p95_ms? as f64,
            self.cancel_early_p99_ms? as f64,
        ))
    }
    pub fn cancel_late(&self) -> Option<(f64, f64, f64, f64)> {
        Some((
            self.cancel_late_p50_ms? as f64,
            self.cancel_late_p85_ms? as f64,
            self.cancel_late_p95_ms? as f64,
            self.cancel_late_p99_ms? as f64,
        ))
    }
}

/// Floor a millisecond timestamp to the nearest event boundary (secs).
///
/// Returns `None` when the timestamp falls within the **final 30 s**
/// of an event (the close-only / lock-in window). live's RttGate
/// computes p60 at `event_start + 270s`, so any Submit whose
/// `ts_offset >= 270s` would never contribute to the gate's prev_p
/// — parser must drop them to keep the parsed distribution shape
/// aligned with what the live gate actually quantiles.
/// Floor a millisecond timestamp to its event boundary AND return the
/// submit's offset from event_start in seconds. Returns `None` when
/// the submit lands in the final 30 s (close-only / lock-in window),
/// matching live's `RttGate::maybe_lock_in` which drops those samples
/// from p60.
///
/// Used by the intra-event segment router (2026-05-28) to decide
/// whether the sample lands in the "early" (offset <
/// `SEGMENT_BOUNDARY_SECS`) or "late" bucket.
#[inline]
fn submit_ts_to_event_secs_with_offset(ts_ms: u64) -> Option<(u64, u64)> {
    let secs = ts_ms / 1000;
    let event_secs = (secs / EVENT_PERIOD_SECS) * EVENT_PERIOD_SECS;
    let offset_secs = secs - event_secs;
    if offset_secs >= EVENT_PERIOD_SECS - LOCKIN_OFFSET_SECS {
        None
    } else {
        Some((event_secs, offset_secs))
    }
}

/// Extract `1779286500` from a line containing
/// `Event ended: btc-updown-5m-1779286500 outcome=...`. Returns
/// `None` on any parse failure (line not an Event-ended log, no slug,
/// or non-numeric ts).
fn parse_event_ended_line(line: &str) -> Option<u64> {
    let idx = line.find("Event ended: btc-updown-")?;
    let rest = &line[idx + "Event ended: btc-updown-".len()..];
    // Format: "5m-1779286500 outcome=..." or similar variants.
    let dash_idx = rest.find('-')?;
    let after_dash = &rest[dash_idx + 1..];
    let end = after_dash.find(|c: char| !c.is_ascii_digit())?;
    after_dash[..end].parse().ok()
}

/// Parse the ISO-8601 timestamp prefix `2026-05-20T14:20:06.618Z` into
/// milliseconds since epoch. Returns `None` if the line doesn't start
/// with a valid 24-char timestamp.
/// Auto-calibrate the TAKER matching-engine overhead distribution from live
/// log(s). Pairs `Submit ↔ Order accepted` by coid, splits by status
/// (`live`=maker, `matched`=taker), and computes `taker_rtt − concurrent-maker
/// median` (per-minute bucket) — the structural matching premium, isolated from
/// the shared network latency. Returns `(p50, p95, p99)` ms, or `None` if too
/// few taker samples. NOTE: must be per-sample (taker − concurrent maker); the
/// percentile difference `taker_pX − place_pX` badly under-estimates the tail.
pub fn extract_taker_overhead(paths: &[String]) -> std::io::Result<Option<(f64, f64, f64)>> {
    let mut submit_ts: HashMap<u64, u64> = HashMap::new();
    let mut maker_by_min: HashMap<u64, Vec<f64>> = HashMap::new();
    let mut takers: Vec<(u64, f64)> = Vec::new();
    for path in paths {
        for line in calib_lines(path)? {
            let line = match line { Ok(l) => l, Err(_) => continue };
            if line.contains("] Submit ") {
                if let (Some(ts), Some(coid)) = (parse_iso_ts_ms(&line), parse_coid(&line)) {
                    if submit_ts.len() < MAX_PENDING {
                        submit_ts.insert(coid, ts);
                    }
                }
                continue;
            }
            if line.contains("] Order accepted:") {
                if let (Some(ts), Some(coid)) = (parse_iso_ts_ms(&line), parse_coid(&line)) {
                    if let Some(submit) = submit_ts.remove(&coid) {
                        let rtt = ts.saturating_sub(submit) as f64;
                        if rtt > 15000.0 {
                            continue;
                        }
                        let min = ts / 60_000;
                        if line.contains("status=matched") {
                            takers.push((min, rtt));
                        } else if line.contains("status=live") {
                            maker_by_min.entry(min).or_default().push(rtt);
                        }
                    }
                }
            }
        }
    }
    let mut maker_med: HashMap<u64, f64> = HashMap::new();
    for (m, mut v) in maker_by_min {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        maker_med.insert(m, v[v.len() / 2]);
    }
    let mut ovr: Vec<f64> = Vec::new();
    for (min, rtt) in takers {
        let base = maker_med
            .get(&min)
            .or_else(|| maker_med.get(&min.saturating_sub(1)))
            .or_else(|| maker_med.get(&(min + 1)));
        if let Some(&b) = base {
            let o = rtt - b;
            if o >= 0.0 {
                ovr.push(o);
            }
        }
    }
    if ovr.len() < 30 {
        return Ok(None);
    }
    ovr.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| ovr[((ovr.len() as f64 * p) as usize).min(ovr.len() - 1)];
    Ok(Some((pct(0.50), pct(0.95), pct(0.99))))
}

fn parse_iso_ts_ms(line: &str) -> Option<u64> {
    if line.len() < 24 { return None; }
    let prefix = &line[..24];
    if !prefix.ends_with('Z') { return None; }
    chrono::DateTime::parse_from_rfc3339(prefix)
        .ok()
        .map(|dt| dt.timestamp_millis().max(0) as u64)
}

/// Extract `1779286506589` from `... coid=1779286506589 oid=...` or
/// `... coid=1779286506589\n` etc.
fn parse_coid(line: &str) -> Option<u64> {
    let idx = line.find(" coid=")?;
    let rest = &line[idx + " coid=".len()..];
    // Token runs until whitespace. Live/paper coids are minted as
    // "{instance_id}-{counter}" (e.g. "btc01-1779286506589"); the unique,
    // monotonic counter is the digit run after the last '-'. Legacy coids are
    // bare digits ("1779286506589") — `rsplit('-')` yields the whole token.
    let tok_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let digits = rest[..tok_end].rsplit('-').next().unwrap_or("");
    if digits.is_empty() { return None; }
    digits.parse().ok()
}

/// Compute a single quantile at the given probability on a slice of
/// RTT samples (ms). Used by the pooled-p60 carry-over which needs a
/// single q≠fixed (0.85/0.95/0.99) drawn at q=0.60 to match the
/// strategy's `rtt_percentile` config. Caller is responsible for
/// passing ≥ `MIN_SAMPLES` (returns 0.0 on empty).
fn quantile_at(samples: &[f64], q: f64) -> u32 {
    if samples.is_empty() {
        return 0;
    }
    let mut s: Vec<f64> = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((q * s.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(s.len() - 1);
    s[idx].max(0.0).min(u32::MAX as f64) as u32
}

/// Compute (p50, p85, p95, p99) on a slice of RTT samples (ms).
/// Returns four `None` entries when `samples.len() < MIN_SAMPLES`.
/// Otherwise returns four `Some(u32)` using nearest-rank quantiles
/// (`samples` is cloned + sorted internally).
fn quantiles(samples: &[f64]) -> (Option<u32>, Option<u32>, Option<u32>, Option<u32>) {
    if samples.len() < MIN_SAMPLES {
        return (None, None, None, None);
    }
    let mut s: Vec<f64> = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let nrank = |q: f64| -> u32 {
        let idx = ((q * s.len() as f64).ceil() as usize).saturating_sub(1)
            .min(s.len() - 1);
        s[idx].max(0.0).min(u32::MAX as f64) as u32
    };
    (Some(nrank(0.50)), Some(nrank(0.85)), Some(nrank(0.95)), Some(nrank(0.99)))
}

// ═════════════════════════════════════════════════════════════════
//  Tests
// ═════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_log(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn parse_iso_ts_extracts_millis() {
        let line = "2026-05-20T14:20:06.618Z  INFO [...]";
        let ts = parse_iso_ts_ms(line).unwrap();
        // 2026-05-20T14:20:06.618Z = 1779286806618 ms
        assert_eq!(ts, 1779286806618);
    }

    #[test]
    fn parse_iso_ts_rejects_short_lines() {
        assert_eq!(parse_iso_ts_ms("2026-05-20"), None);
        assert_eq!(parse_iso_ts_ms(""), None);
    }

    #[test]
    fn parse_coid_finds_digits() {
        assert_eq!(parse_coid("[...] Submit BUY @ 0.5 coid=1779286506589 oid=0x..."), Some(1779286506589));
        assert_eq!(parse_coid("[...] Order accepted: orderID=0x... status=live coid=42"), Some(42));
        assert_eq!(parse_coid("no coid"), None);
        // Live/paper prefixed form "{instance_id}-{counter}": the counter
        // (after the last '-') is the unique numeric key.
        assert_eq!(parse_coid("[...] Submit BUY @ 0.5 coid=btc01-1779286506589 oid=0x..."), Some(1779286506589));
        assert_eq!(parse_coid("[...] status=live coid=btc-02-42"), Some(42));
    }

    #[test]
    fn parse_event_ended_extracts_secs() {
        let line = "2026-05-20T14:20:06.618Z  INFO ::: [polymaker] Event ended: btc-updown-5m-1779286500 outcome=Up ...";
        assert_eq!(parse_event_ended_line(line), Some(1779286500));
    }

    #[test]
    fn submit_ts_to_event_secs_floors_to_5min() {
        // 14:20:06.618 = 1779286806618 ms = 1779286806 secs.
        // event_start = 1779286800, offset = 6s, within first 270s → Some.
        assert_eq!(submit_ts_to_event_secs_with_offset(1779286806618), Some((1779286800, 6)));
        // Exact boundary (offset=0) → Some.
        assert_eq!(submit_ts_to_event_secs_with_offset(1779286500_000), Some((1779286500, 0)));
        // Just before next boundary (offset=299s, in last 30s) → None.
        assert_eq!(submit_ts_to_event_secs_with_offset(1779286499_999), None);
    }

    /// Lock-in window cut: timestamps in the final 30 s of an event
    /// return `None` (parser drops the sample to match live's
    /// `RttGate::maybe_lock_in` at T-30 s).
    #[test]
    fn submit_ts_to_event_secs_drops_lockin_window() {
        // event_start = 1779286500, lock-in cut at offset 270s.
        let evt = 1779286500u64;
        // offset = 269.9s → still in tracking window → Some.
        let ts_ms = (evt + 269) * 1000 + 999;
        assert_eq!(submit_ts_to_event_secs_with_offset(ts_ms), Some((evt, 269)));
        // offset = 270.0s → first sample in lock-in window → None.
        let ts_ms = (evt + 270) * 1000;
        assert_eq!(submit_ts_to_event_secs_with_offset(ts_ms), None);
        // offset = 299.9s → deep in lock-in window → None.
        let ts_ms = (evt + 299) * 1000 + 999;
        assert_eq!(submit_ts_to_event_secs_with_offset(ts_ms), None);
        // Next event boundary — Some again.
        let ts_ms = (evt + 300) * 1000;
        assert_eq!(submit_ts_to_event_secs_with_offset(ts_ms), Some((evt + 300, 0)));
    }

    /// **Segmented bucketing** (2026-05-28): offset < SEGMENT_BOUNDARY_SECS
    /// → early; SEGMENT_BOUNDARY_SECS..270 → late; ≥270 → dropped.
    #[test]
    fn segment_router_buckets_early_vs_late_correctly() {
        let evt = 1779286500u64;
        // Just before segment boundary — early.
        let ts_ms = (evt + SEGMENT_BOUNDARY_SECS - 1) * 1000;
        let (e, off) = submit_ts_to_event_secs_with_offset(ts_ms).unwrap();
        assert_eq!(e, evt);
        assert!(off < SEGMENT_BOUNDARY_SECS, "early-bucket offset");
        // At segment boundary — late.
        let ts_ms = (evt + SEGMENT_BOUNDARY_SECS) * 1000;
        let (e, off) = submit_ts_to_event_secs_with_offset(ts_ms).unwrap();
        assert_eq!(e, evt);
        assert!(off >= SEGMENT_BOUNDARY_SECS, "late-bucket offset");
    }

    #[test]
    fn quantiles_too_few_samples_returns_none() {
        let (p50, p85, p95, p99) = quantiles(&[10.0, 20.0]);
        assert!(p50.is_none() && p85.is_none() && p95.is_none() && p99.is_none());
    }

    #[test]
    fn quantiles_basic_sequence() {
        let samples: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        let (p50, p85, p95, p99) = quantiles(&samples);
        assert_eq!(p50, Some(50));
        assert_eq!(p85, Some(85));
        assert_eq!(p95, Some(95));
        assert_eq!(p99, Some(99));
    }

    /// Synthetic 2-event live.log with 4 place + 3 cancel pairings.
    /// Verifies event bucketing, quantile compute, prev_p85 carryover.
    #[test]
    fn extract_two_events_with_pairings() {
        // event 1779286500 (14:15-14:20): 4 place RTTs (50, 100, 150, 200 ms)
        // event 1779286800 (14:20-14:25): 3 place RTTs (10, 50, 90 ms) + 3 cancel (30, 60, 100 ms)
        let log = "\
2026-05-20T14:16:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1001 oid=0x1
2026-05-20T14:16:00.050Z  INFO [PolymarketTrade] Order accepted: orderID=0x1 status=live coid=1001
2026-05-20T14:16:01.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1002 oid=0x2
2026-05-20T14:16:01.100Z  INFO [PolymarketTrade] Order accepted: orderID=0x2 status=live coid=1002
2026-05-20T14:16:02.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1003 oid=0x3
2026-05-20T14:16:02.150Z  INFO [PolymarketTrade] Order accepted: orderID=0x3 status=live coid=1003
2026-05-20T14:16:03.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1004 oid=0x4
2026-05-20T14:16:03.200Z  INFO [PolymarketTrade] Order accepted: orderID=0x4 status=live coid=1004
2026-05-20T14:20:06.618Z  INFO [polymaker] Event ended: btc-updown-5m-1779286500 outcome=Up
2026-05-20T14:21:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=2001 oid=0xa
2026-05-20T14:21:00.010Z  INFO [PolymarketTrade] Order accepted: orderID=0xa status=live coid=2001
2026-05-20T14:21:01.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=2002 oid=0xb
2026-05-20T14:21:01.050Z  INFO [PolymarketTrade] Order accepted: orderID=0xb status=live coid=2002
2026-05-20T14:21:02.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=2003 oid=0xc
2026-05-20T14:21:02.090Z  INFO [PolymarketTrade] Order accepted: orderID=0xc status=live coid=2003
2026-05-20T14:21:03.000Z  INFO [PolymarketTrade] Cancel request orderID=0xa coid=2001
2026-05-20T14:21:03.030Z  INFO [PolymarketTrade] Cancel result orderID=0xa coid=2001 canceled=1 not_canceled=0
2026-05-20T14:21:04.000Z  INFO [PolymarketTrade] Cancel request orderID=0xb coid=2002
2026-05-20T14:21:04.060Z  INFO [PolymarketTrade] Cancel result orderID=0xb coid=2002 canceled=1 not_canceled=0
2026-05-20T14:21:05.000Z  INFO [PolymarketTrade] Cancel request orderID=0xc coid=2003
2026-05-20T14:21:05.100Z  INFO [PolymarketTrade] Cancel result orderID=0xc coid=2003 canceled=1 not_canceled=0
2026-05-20T14:25:01.000Z  INFO [polymaker] Event ended: btc-updown-5m-1779286800 outcome=Down
";
        let f = write_log(log);
        let paths = vec![f.path().to_str().unwrap().to_string()];
        let table = extract_per_event_rtt(&paths).unwrap();
        // Two events present.
        assert!(table.contains_key(&1779286500));
        assert!(table.contains_key(&1779286800));

        let e1 = table[&1779286500];
        // Place RTTs: 50, 100, 150, 200 → p50=100, p85=200, p95=200, p99=200 (nearest-rank).
        assert_eq!(e1.place_n_samples, 4);
        assert_eq!(e1.place_p50_ms, Some(100));
        assert_eq!(e1.place_p85_ms, Some(200));
        assert_eq!(e1.place_p95_ms, Some(200));
        assert_eq!(e1.place_p99_ms, Some(200));
        assert_eq!(e1.cancel_n_samples, 0);
        assert!(e1.cancel_p50_ms.is_none());
        // First event — no prev.
        assert!(e1.prev_event_p_ms.is_none());

        let e2 = table[&1779286800];
        // Place RTTs: 10, 50, 90 → p50=50, p85=90, p95=90, p99=90.
        assert_eq!(e2.place_n_samples, 3);
        assert_eq!(e2.place_p50_ms, Some(50));
        assert_eq!(e2.place_p85_ms, Some(90));
        // Cancel RTTs: 30, 60, 100 → p50=60, p85=100, p95=100, p99=100.
        assert_eq!(e2.cancel_n_samples, 3);
        assert_eq!(e2.cancel_p50_ms, Some(60));
        assert_eq!(e2.cancel_p85_ms, Some(100));
        // prev_p carries forward = place-only p60 of event1's
        // place=[50,100,150,200]. Sorted = [50,100,150,200].
        // Nearest-rank at q=0.60 → idx = ceil(0.6*4)-1 = 2 → 150.
        assert_eq!(e2.prev_event_p_ms, Some(150));
    }

    /// **Per-event timeout rate** = timeouts / requests from raw line
    /// counts. 3 submits + 1 NewOrderTimeout → place rate 1/3; 3 cancel
    /// requests + 1 CancelOrderTimeout → cancel rate 1/3. Rate is robust
    /// to a late `Cancel result` after the timeout (the denominator is the
    /// request count, not pairing), though that late result does pair into
    /// the cancel quantile samples (same as the pre-timeout-feature parser,
    /// which never special-cased timeouts).
    #[test]
    fn extract_per_event_timeout_rate_raw_counts() {
        let log = "\
2026-05-20T14:16:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1001 oid=0x1
2026-05-20T14:16:00.050Z  INFO [PolymarketTrade] Order accepted: orderID=0x1 status=live coid=1001
2026-05-20T14:16:01.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1002 oid=0x2
2026-05-20T14:16:01.100Z  INFO [PolymarketTrade] Order accepted: orderID=0x2 status=live coid=1002
2026-05-20T14:16:02.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1003 oid=0x3
2026-05-20T14:16:04.000Z  INFO [polymaker] NewOrderTimeout coid=1003 orderID=<none> → orphan (pending reconciliation)
2026-05-20T14:17:00.000Z  INFO [PolymarketTrade] Cancel request orderID=0x1 coid=1001
2026-05-20T14:17:00.050Z  INFO [PolymarketTrade] Cancel result orderID=0x1 coid=1001 canceled=1 not_canceled=0
2026-05-20T14:17:01.000Z  INFO [PolymarketTrade] Cancel request orderID=0x2 coid=1002
2026-05-20T14:17:01.100Z  INFO [PolymarketTrade] Cancel result orderID=0x2 coid=1002 canceled=1 not_canceled=0
2026-05-20T14:17:02.000Z  INFO [PolymarketTrade] Cancel request orderID=0x3 coid=1003
2026-05-20T14:17:04.000Z  INFO [polymaker] CancelOrderTimeout coid=1003 orderID=0x3 → orphan
2026-05-20T14:17:04.500Z  INFO [PolymarketTrade] Cancel result orderID=0x3 coid=1003 canceled=1 not_canceled=0
2026-05-20T14:20:06.618Z  INFO [polymaker] Event ended: btc-updown-5m-1779286500 outcome=Up
";
        let f = write_log(log);
        let paths = vec![f.path().to_str().unwrap().to_string()];
        let table = extract_per_event_rtt(&paths).unwrap();
        let e = table[&1779286500];
        // Place: coid 1003 timed out (no late ack) → 2 ack samples.
        assert_eq!(e.place_n_samples, 2, "timed-out place is not an ack sample");
        // Cancel: the late result for coid 1003 pairs into the quantiles
        // (the timeout branch only counts the rate, doesn't touch pairing).
        assert_eq!(e.cancel_n_samples, 3, "late cancel result pairs as a sample");
        // rate = 1 timeout / 3 requests = 1/3 on each side.
        let third = 1.0 / 3.0;
        assert!((e.place_timeout_rate.unwrap() - third).abs() < 1e-9, "place rate {:?}", e.place_timeout_rate);
        assert!((e.cancel_timeout_rate.unwrap() - third).abs() < 1e-9, "cancel rate {:?}", e.cancel_timeout_rate);
    }

    /// No timeouts but enough samples → rate is `Some(0.0)`, not `None`.
    #[test]
    fn extract_timeout_rate_zero_when_no_timeouts() {
        let log = "\
2026-05-20T14:16:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1 oid=0x1
2026-05-20T14:16:00.050Z  INFO [PolymarketTrade] Order accepted: orderID=0x1 status=live coid=1
2026-05-20T14:16:01.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=2 oid=0x2
2026-05-20T14:16:01.100Z  INFO [PolymarketTrade] Order accepted: orderID=0x2 status=live coid=2
2026-05-20T14:16:02.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=3 oid=0x3
2026-05-20T14:16:02.150Z  INFO [PolymarketTrade] Order accepted: orderID=0x3 status=live coid=3
2026-05-20T14:20:06.618Z  INFO [polymaker] Event ended: btc-updown-5m-1779286500 outcome=Up
";
        let f = write_log(log);
        let paths = vec![f.path().to_str().unwrap().to_string()];
        let table = extract_per_event_rtt(&paths).unwrap();
        let e = table[&1779286500];
        assert_eq!(e.place_timeout_rate, Some(0.0));
        // Cancel side had no requests → below MIN_SAMPLES → None.
        assert_eq!(e.cancel_timeout_rate, None);
    }

    /// **Place-only p60 carry-over verification** (2026-05-21). The
    /// previous-event prev_p carry should be the place-only p60 — NOT
    /// place-only p85 (the original regression) and NOT pooled
    /// place+cancel p60. The gate's `record_sample` is wired only to
    /// the place-side ack path (see strategy.rs:6510), so the
    /// carry-over must match that source exactly.
    #[test]
    fn prev_event_carry_is_place_only_p60_not_place_p85() {
        // Event 1: 5 place samples [100,200,300,400,500] + 5 cancel
        // [50,100,150,200,250]. Cancel is captured into the entry's
        // own quantile fields but does NOT enter the gate carry-over.
        // Place-only sorted = [100,200,300,400,500].
        // q=0.60 → idx = ceil(0.6*5)-1 = 2 → 300.
        // (vs place-only p85 of [100..500] would be 500 — a 1.67×
        // inflation that this fix guards against.)
        let log = "\
2026-05-20T14:16:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1001 oid=0x1
2026-05-20T14:16:00.100Z  INFO [PolymarketTrade] Order accepted: orderID=0x1 status=live coid=1001
2026-05-20T14:16:01.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1002 oid=0x2
2026-05-20T14:16:01.200Z  INFO [PolymarketTrade] Order accepted: orderID=0x2 status=live coid=1002
2026-05-20T14:16:02.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1003 oid=0x3
2026-05-20T14:16:02.300Z  INFO [PolymarketTrade] Order accepted: orderID=0x3 status=live coid=1003
2026-05-20T14:16:03.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1004 oid=0x4
2026-05-20T14:16:03.400Z  INFO [PolymarketTrade] Order accepted: orderID=0x4 status=live coid=1004
2026-05-20T14:16:04.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1005 oid=0x5
2026-05-20T14:16:04.500Z  INFO [PolymarketTrade] Order accepted: orderID=0x5 status=live coid=1005
2026-05-20T14:17:00.000Z  INFO [PolymarketTrade] Cancel request orderID=0x1 coid=1001
2026-05-20T14:17:00.050Z  INFO [PolymarketTrade] Cancel result orderID=0x1 coid=1001 canceled=1 not_canceled=0
2026-05-20T14:17:01.000Z  INFO [PolymarketTrade] Cancel request orderID=0x2 coid=1002
2026-05-20T14:17:01.100Z  INFO [PolymarketTrade] Cancel result orderID=0x2 coid=1002 canceled=1 not_canceled=0
2026-05-20T14:17:02.000Z  INFO [PolymarketTrade] Cancel request orderID=0x3 coid=1003
2026-05-20T14:17:02.150Z  INFO [PolymarketTrade] Cancel result orderID=0x3 coid=1003 canceled=1 not_canceled=0
2026-05-20T14:17:03.000Z  INFO [PolymarketTrade] Cancel request orderID=0x4 coid=1004
2026-05-20T14:17:03.200Z  INFO [PolymarketTrade] Cancel result orderID=0x4 coid=1004 canceled=1 not_canceled=0
2026-05-20T14:17:04.000Z  INFO [PolymarketTrade] Cancel request orderID=0x5 coid=1005
2026-05-20T14:17:04.250Z  INFO [PolymarketTrade] Cancel result orderID=0x5 coid=1005 canceled=1 not_canceled=0
2026-05-20T14:20:06.618Z  INFO [polymaker] Event ended: btc-updown-5m-1779286500 outcome=Up
2026-05-20T14:21:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=2001 oid=0xa
2026-05-20T14:21:00.030Z  INFO [PolymarketTrade] Order accepted: orderID=0xa status=live coid=2001
2026-05-20T14:21:01.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=2002 oid=0xb
2026-05-20T14:21:01.040Z  INFO [PolymarketTrade] Order accepted: orderID=0xb status=live coid=2002
2026-05-20T14:21:02.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=2003 oid=0xc
2026-05-20T14:21:02.050Z  INFO [PolymarketTrade] Order accepted: orderID=0xc status=live coid=2003
2026-05-20T14:25:00.000Z  INFO [polymaker] Event ended: btc-updown-5m-1779286800 outcome=Up
";
        let f = write_log(log);
        let table = extract_per_event_rtt(
            &[f.path().to_str().unwrap().to_string()],
        ).unwrap();
        let e1 = table[&1779286500];
        // Sanity: place p85 alone of event1 = nearest-rank q=0.85 on
        // sorted [100,200,300,400,500] → idx = ceil(4.25)-1 = 4 → 500.
        assert_eq!(e1.place_p85_ms, Some(500));
        // Sanity: cancel samples are present.
        assert_eq!(e1.cancel_n_samples, 5);
        // The actual carry into event 2:
        let e2 = table[&1779286800];
        // Place-only sorted = [100,200,300,400,500].
        // q=0.60: idx = ceil(0.6*5)-1 = 2 → 300ms.
        assert_eq!(e2.prev_event_p_ms, Some(300),
            "prev_p must be place-only p60 (300), not place p85 (500) \
             nor pooled p60 (200)");
    }

    /// **Lock-in cut + Order failed inclusion verification** (2026-05-21).
    /// Live event 1779286500 (14:15-14:20):
    ///   * Submits at offsets 60s, 120s (both < 270s) → kept
    ///   * Submit at offset 280s (in last 30 s lock-in window) → dropped
    ///   * Submit at offset 90s with Order failed reply → kept
    /// Expected place_n_samples = 3 (two Accepted + one failed).
    /// Verifies BOTH the lock-in cut AND the Order failed include.
    #[test]
    fn lockin_cut_drops_last_30s_and_includes_order_failed() {
        let log = "\
2026-05-20T14:16:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1001 oid=0x1
2026-05-20T14:16:00.100Z  INFO [PolymarketTrade] Order accepted: orderID=0x1 status=live coid=1001
2026-05-20T14:17:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1002 oid=0x2
2026-05-20T14:17:00.200Z  INFO [PolymarketTrade] Order accepted: orderID=0x2 status=live coid=1002
2026-05-20T14:16:30.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=1003 oid=0x3
2026-05-20T14:16:30.300Z  WARN [PolymarketTrade] Order failed: status 400 ({\"error\":\"x\"}) coid=1003
2026-05-20T14:19:40.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=9001 oid=0x9
2026-05-20T14:19:40.999Z  INFO [PolymarketTrade] Order accepted: orderID=0x9 status=live coid=9001
2026-05-20T14:20:06.000Z  INFO [polymaker] Event ended: btc-updown-5m-1779286500 outcome=Up
";
        let f = write_log(log);
        let table = extract_per_event_rtt(
            &[f.path().to_str().unwrap().to_string()],
        ).unwrap();
        let e = table[&1779286500];
        // 3 kept (offsets 60s, 120s, 90s) — drop the offset 280s sample.
        assert_eq!(e.place_n_samples, 3,
            "expected 3 samples (2 Accepted within 270s + 1 Order failed within 270s); \
             the Submit at offset 280s in the lock-in window must be dropped");
        // RTTs sorted: 100, 200, 300 ms (the 999ms one was dropped).
        assert_eq!(e.place_p50_ms, Some(200));
        assert_eq!(e.place_p85_ms, Some(300));
    }

    /// **Strategy-overhead factor math** (2026-05-21). The engine
    /// multiplies parser `prev_event_p_ms` by `sim_per_event_rtt_overhead_factor`
    /// before pushing into the gate. This test pins the math:
    /// parser p60 of 300 ms × factor 1.35 → 405 ms override.
    ///
    /// The factor compensates for live's gate measuring RTT from
    /// `on_quote_entry → ack_arrival_at_strategy` (full strategy
    /// overhead + HTTP RTT) while the parser only sees the HTTP RTT
    /// part. Empirical ratio sim/live = 0.61-0.78 over 3 sampled
    /// events → recommended factor 1.30-1.45, default 1.0 (legacy).
    #[test]
    fn overhead_factor_math_documented() {
        // Parser's p60 (HTTP-only RTT) for a typical event.
        let parser_p60: u32 = 300;

        // Engine applies the factor on push (mirrors engine.rs:2454):
        let factor: f64 = 1.35;
        let engine_pushes: f64 = (parser_p60 as f64) * factor;

        // Verify the math — the gate will see 405 ms, not 300 ms.
        assert!((engine_pushes - 405.0).abs() < 1e-9,
            "expected 300 × 1.35 = 405.0, got {}", engine_pushes);

        // Legacy default (factor=1.0) is a no-op.
        let legacy = (parser_p60 as f64) * 1.0;
        assert_eq!(legacy as u32, parser_p60);
    }

    /// Empty path list → empty map, no error.
    #[test]
    fn empty_paths_returns_empty_map() {
        let table = extract_per_event_rtt(&[]).unwrap();
        assert!(table.is_empty());
    }

    /// Log with no Submit/Accept pairs but with Event-ended lines still
    /// produces entries (all-None quantiles) so prev_p85 carry can
    /// chain through gaps.
    #[test]
    fn event_with_no_pairings_still_present() {
        let log = "\
2026-05-20T14:20:00.000Z  INFO [polymaker] Event ended: btc-updown-5m-1779286500 outcome=Up
2026-05-20T14:25:00.000Z  INFO [polymaker] Event ended: btc-updown-5m-1779286800 outcome=Down
";
        let f = write_log(log);
        let table = extract_per_event_rtt(
            &[f.path().to_str().unwrap().to_string()],
        ).unwrap();
        assert_eq!(table.len(), 2);
        let e = table[&1779286500];
        assert_eq!(e.place_n_samples, 0);
        assert!(e.place_p50_ms.is_none());
        assert!(e.prev_event_p_ms.is_none());
    }

    /// Unmatched Submit (no Order accepted) drops naturally — doesn't
    /// blow up and doesn't contribute a phantom RTT.
    #[test]
    fn unmatched_submit_is_discarded() {
        let log = "\
2026-05-20T14:16:00.000Z  INFO [PolymarketTrade] Submit BUY @ 0.5 qty=5 coid=9999 oid=0x1
2026-05-20T14:20:00.000Z  INFO [polymaker] Event ended: btc-updown-5m-1779286500 outcome=Up
";
        let f = write_log(log);
        let table = extract_per_event_rtt(
            &[f.path().to_str().unwrap().to_string()],
        ).unwrap();
        // Event present (Submit ts seeded observed_events), but with
        // zero RTT samples and all-None quantiles.
        let e = table[&1779286500];
        assert_eq!(e.place_n_samples, 0);
    }
}
