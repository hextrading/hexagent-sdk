//! Thin wrapper over the existing `src/exchange/sim/latency.rs` coupled
//! sampler. v2 reuses the proven empirical-CDF + AR(1) machinery; this just
//! adapts it to the v2 split convention.
//!
//! `sample_place` / `sample_cancel` return a full RTT (post-2026-05-15 sim
//! refactor; same as the engine's `coupled` usage). We split 50/50 into the
//! outbound L1 (emit → matching core) and inbound L2 (core → strategy),
//! mirroring v1's `signal_rtt_split`.

use crate::exchange::sim::latency::{CoupledLatencySamplers, LatencyProfile, LatencySampler};
use crate::exchange::sim::per_event_rtt::{EventRttOverride, SEGMENT_BOUNDARY_SECS};

pub struct LatencyModel {
    coupled: CoupledLatencySamplers,
    fill_push_mult: f64,
    /// TAKER matching-engine overhead, added on top of the (time-varying) place
    /// RTT for taker fills (additive model: taker ≈ place(now) + overhead).
    overhead: LatencySampler,
    seed: u64,
    /// Client timeout (ms) — the censoring threshold. Per-event timeout rates
    /// are turned into an exceedance anchor at this cap so `P(RTT > cap)`
    /// matches live (see `empirical_anchors_censored`). Defaults to 2000 ms;
    /// the simulator overrides it from `client_timeout_ns`.
    client_timeout_ms: f64,
}

impl LatencyModel {
    pub fn new(place: LatencyProfile, cancel: LatencyProfile, rho_cross: f64, seed: u64) -> Self {
        let place_sampler = LatencySampler::new(place, seed.wrapping_add(0xC0FFEE));
        let cancel_sampler = LatencySampler::new(cancel, seed.wrapping_add(0xCAFE_BABE));
        let coupled =
            CoupledLatencySamplers::new(place_sampler, cancel_sampler, rho_cross, seed.wrapping_add(0xC0DE_C0DE));
        // Default taker overhead anchors (live 2026-05-28: taker − concurrent
        // maker p50/p95/p99 ≈ 267/910/1612 ms). rho small — overhead is roughly
        // independent of the network/maker level (measured corr ≈ 0.22).
        let overhead = LatencySampler::new(Self::empirical_profile(267.0, 910.0, 1612.0, 0.3), seed.wrapping_add(0x0FACE));
        Self { coupled, fill_push_mult: 1.5, overhead, seed, client_timeout_ms: 2000.0 }
    }

    pub fn set_fill_push_mult(&mut self, mult: f64) {
        if mult > 0.0 {
            self.fill_push_mult = mult;
        }
    }

    /// Set the client-timeout cap (ms) used as the per-event exceedance
    /// threshold. Must match the simulator's `client_timeout_ns` so the
    /// injected tail times out at exactly the same boundary the engine
    /// checks (`rtt > client_timeout_ns`).
    pub fn set_client_timeout_ms(&mut self, ms: f64) {
        if ms > 0.0 && ms.is_finite() {
            self.client_timeout_ms = ms;
        }
    }

    /// Set the taker matching-overhead distribution (ms quantiles).
    pub fn set_taker_overhead_anchors(&mut self, p50_ms: f64, p95_ms: f64, p99_ms: f64) {
        if p50_ms > 0.0 {
            self.overhead = LatencySampler::new(
                Self::empirical_profile(p50_ms, p95_ms.max(p50_ms + 1.0), p99_ms.max(p95_ms + 1.0), 0.3),
                self.seed.wrapping_add(0x0FACE),
            );
        }
    }

    /// Sample the taker matching-engine overhead (ns) to add to a taker fill's
    /// delivery latency. The place RTT (already time-varying / per-event)
    /// supplies the shared network component, so taker co-moves with maker.
    pub fn sample_taker_overhead(&mut self, now_ns: u64) -> u64 {
        self.overhead.sample_ns(now_ns)
    }

    /// Apply a per-event RTT override (sim_rtt_mode="exact"), mirroring v1's
    /// sim-lane push: intra-event segmented (early/late) anchors when both
    /// buckets have samples, else aggregate per-event quantiles; place/cancel
    /// gated independently. Stays in effect until the next event replaces or
    /// clears it.
    pub fn apply_per_event_override(&mut self, entry: &EventRttOverride, event_secs: u64) {
        let event_start_ns = event_secs.saturating_mul(1_000_000_000);
        // Per-event timeout rates re-inject the right-censored tail the
        // non-timeout quantiles omit (cancel timeouts ≈ 0 in sim vs ~1.3 %
        // in live; place tail truncated). `cap` is the engine's timeout
        // boundary so the injected mass lands exactly past `rtt > cap`.
        let pr = entry.place_timeout_rate;
        let cr = entry.cancel_timeout_rate;
        let cap = self.client_timeout_ms;
        let q = |p50: Option<u32>, p85: Option<u32>, p95: Option<u32>, p99: Option<u32>| match (p50, p85, p95, p99) {
            (Some(a), Some(b), Some(c), Some(d)) => Some((a as f64, b as f64, c as f64, d as f64)),
            _ => None,
        };
        let place_seg = if entry.has_segmented_place() {
            Some((entry.place_early().unwrap(), entry.place_late().unwrap()))
        } else {
            None
        };
        let cancel_seg = if entry.has_segmented_cancel() {
            Some((entry.cancel_early().unwrap(), entry.cancel_late().unwrap()))
        } else {
            None
        };
        // Each side resolved INDEPENDENTLY (segmented → aggregate → clear)
        // via per-side setters. The old code used the two-sided setters in a
        // segmented-then-fallback layering, where the aggregate fallback for
        // one side passed `None` for the other and CLEARED its just-set
        // anchor (the partial-segmented clearing bug). Per-side calls touch
        // exactly one sampler, so "place segmented, cancel aggregate" (and
        // every other mix) is expressed without clobber.
        if let Some((early, late)) = place_seg {
            self.coupled.set_place_per_event_segmented_anchors(event_start_ns, early, late, SEGMENT_BOUNDARY_SECS, pr, cap);
        } else if let Some((p50, p85, p95, p99)) = q(entry.place_p50_ms, entry.place_p85_ms, entry.place_p95_ms, entry.place_p99_ms) {
            self.coupled.set_place_per_event_anchors(p50, p85, p95, p99, pr, cap);
        } else {
            self.coupled.clear_place_per_event_anchors();
        }
        if let Some((early, late)) = cancel_seg {
            self.coupled.set_cancel_per_event_segmented_anchors(event_start_ns, early, late, SEGMENT_BOUNDARY_SECS, cr, cap);
        } else if let Some((p50, p85, p95, p99)) = q(entry.cancel_p50_ms, entry.cancel_p85_ms, entry.cancel_p95_ms, entry.cancel_p99_ms) {
            self.coupled.set_cancel_per_event_anchors(p50, p85, p95, p99, cr, cap);
        } else {
            self.coupled.clear_cancel_per_event_anchors();
        }
    }

    /// Clear per-event anchors → fall back to the pooled/static CDF.
    pub fn clear_per_event_override(&mut self) {
        self.coupled.set_per_event_anchors(None, None, None, None, self.client_timeout_ms);
    }

    /// Build an `Empirical` (5-anchor CDF + AR(1)) profile from p50/p95/p99 ms.
    /// P1 uses static anchors; per-event / calibrate-from-log refinement is P4.
    pub fn empirical_profile(p50_ms: f64, p95_ms: f64, p99_ms: f64, rho: f64) -> LatencyProfile {
        LatencyProfile::Empirical {
            p50_ms,
            p85_ms_override: None,
            p95_ms,
            p99_ms,
            rho,
            p999_ms_override: None,
            gpd_tail: None,
        }
    }

    /// Split a place RTT into (L1 outbound, L2 inbound) ns.
    pub fn sample_place_split(&mut self, now_ns: u64) -> (u64, u64) {
        let rtt = self.coupled.sample_place(now_ns);
        (rtt / 2, rtt - rtt / 2)
    }

    /// Split a cancel RTT into (L1 outbound, L2 inbound) ns.
    pub fn sample_cancel_split(&mut self, now_ns: u64) -> (u64, u64) {
        let rtt = self.coupled.sample_cancel(now_ns);
        (rtt / 2, rtt - rtt / 2)
    }

    /// WebSocket fill-push delay (ns) for a maker/taker fill landing back at
    /// the strategy. Mirrors v1: ~1.5× the half-RTT (server bookkeeping for a
    /// trade event is heavier than a plain place ack). Sampled once per fill.
    pub fn sample_fill_push(&mut self, now_ns: u64) -> u64 {
        let (l1, l2) = self.sample_place_split(now_ns);
        (((l1 + l2) as f64) * 0.5 * self.fill_push_mult) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taker_overhead_sampler_plausible_range() {
        let mut m = LatencyModel::new(LatencyProfile::Fixed(50), LatencyProfile::Fixed(50), 0.0, 7);
        m.set_taker_overhead_anchors(267.0, 910.0, 1612.0);
        let (mut sum, n, mut over_100ms) = (0u64, 500u64, 0u64);
        for i in 0..n {
            let o = m.sample_taker_overhead(1_000_000_000 + i * 1_000_000);
            assert!(o > 10_000_000 && o < 6_000_000_000, "overhead {o} ns out of range");
            sum += o;
            if o > 100_000_000 {
                over_100ms += 1;
            }
        }
        let mean_ms = (sum / n) as f64 / 1e6;
        assert!(mean_ms > 150.0 && mean_ms < 900.0, "mean overhead {mean_ms}ms implausible");
        assert!(over_100ms > n / 2, "expected most draws > 100ms");
    }
}
