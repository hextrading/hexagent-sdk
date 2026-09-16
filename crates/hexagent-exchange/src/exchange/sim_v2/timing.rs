//! Opt-in execution-stage assumptions and bounded, single-writer audit.
//! Recorded HTTP RTT contains processing. The staged model partitions that
//! budget; it never adds processing to an already measured round trip.

use crate::types::{OrderSlot, OrderStatus};
use arrayvec::ArrayString;
use serde::Serialize;
use std::collections::VecDeque;

pub const EXECUTION_TIMING_CAPACITY: usize = 16_384;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelTimingMode {
    #[default]
    LegacyL2Multiplier,
    StagedRttBudget,
    StagedRttFraction,
}

impl std::str::FromStr for CancelTimingMode {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "legacy_l2_multiplier" => Ok(Self::LegacyL2Multiplier),
            "staged_rtt_budget" => Ok(Self::StagedRttBudget),
            "staged_rtt_fraction" => Ok(Self::StagedRttFraction),
            _ => Err(format!("invalid sim_v2_cancel_timing_mode: {value}")),
        }
    }
}

impl CancelTimingMode {
    pub fn is_staged(self) -> bool {
        self != Self::LegacyL2Multiplier
    }
}

/// A modeling assumption, not measured ingress/processing/egress timestamps.
/// Integer arithmetic preserves the total RTT exactly, including odd ns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CancelTiming {
    pub dispatch_ns: u64,
    pub input_l1_ns: u64,
    pub input_l2_ns: u64,
    pub rtt_ns: u64,
    pub network_l1_ns: u64,
    pub processing_requested_ns: u64,
    pub processing_ns: u64,
    pub network_l2_ns: u64,
    pub nominal_arrival_ns: u64,
    pub nominal_effective_ns: u64,
    pub nominal_http_ns: u64,
    pub processing_capped: bool,
    pub evidence_confidence: &'static str,
}

impl CancelTiming {
    /// Bounded, explicitly assumed partition. Keep both measured split legs
    /// nonzero whenever they were nonzero; never saturate a short RTT with a
    /// fixed processing floor. The two residual legs conserve every nanosecond.
    pub fn fraction(dispatch_ns: u64, l1: u64, l2: u64, processing_bps: u16) -> Self {
        assert!(
            processing_bps < 10_000,
            "cancel processing fraction must be below 10000 bps"
        );
        let rtt = l1.checked_add(l2).expect("sim_v2 RTT budget overflow");
        let requested = ((u128::from(rtt) * u128::from(processing_bps) / 10_000) as u64)
            .min(rtt - u64::from(l1 > 0) - u64::from(l2 > 0));
        let mut t = Self::partition(dispatch_ns, l1, l2, requested);
        if l1 > 0 && l2 > 0 {
            let network = rtt - requested;
            t.network_l1_ns = t.network_l1_ns.clamp(1, network - 1);
            t.network_l2_ns = network - t.network_l1_ns;
            t.nominal_arrival_ns = dispatch_ns + t.network_l1_ns;
            t.nominal_effective_ns = t.nominal_arrival_ns + requested;
        }
        t.evidence_confidence = "assumed_fraction_of_observed_rtt";
        t
    }

    pub fn partition(dispatch_ns: u64, l1: u64, l2: u64, requested: u64) -> Self {
        let rtt = l1.checked_add(l2).expect("sim_v2 RTT budget overflow");
        let processing = requested.min(rtt);
        let network = rtt - processing;
        // Round once; derive the other leg by subtraction to conserve RTT.
        let outbound = if rtt == 0 {
            0
        } else {
            ((u128::from(network) * u128::from(l1) + u128::from(rtt / 2)) / u128::from(rtt)) as u64
        };
        let arrival = dispatch_ns
            .checked_add(outbound)
            .expect("sim_v2 arrival clock overflow");
        let effective = arrival
            .checked_add(processing)
            .expect("sim_v2 cancel clock overflow");
        let http = dispatch_ns
            .checked_add(rtt)
            .expect("sim_v2 HTTP clock overflow");
        Self {
            dispatch_ns,
            input_l1_ns: l1,
            input_l2_ns: l2,
            rtt_ns: rtt,
            network_l1_ns: outbound,
            processing_requested_ns: requested,
            processing_ns: processing,
            network_l2_ns: network - outbound,
            nominal_arrival_ns: arrival,
            nominal_effective_ns: effective,
            nominal_http_ns: http,
            processing_capped: requested > rtt,
            evidence_confidence: "estimated_rtt_partition",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStage {
    CancelDispatched,
    CancelArrived,
    CancelEffective,
    HttpScheduled,
    HttpDelivered,
    HttpSuppressed,
    HttpLateDiscarded,
    HttpDeadline,
    PrivateMatched,
    PrivateScheduled,
    PrivateDelivered,
}

/// Inline strings avoid allocations while recording a transition. Oversized
/// identity or a full queue aborts replay, never silently truncates ownership.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ExecutionTimingRow {
    pub schema_version: u32,
    pub stage: ExecutionStage,
    pub coid: ArrayString<128>,
    pub iid: ArrayString<128>,
    pub token: ArrayString<128>,
    pub trade_id: ArrayString<128>,
    pub order_slot: OrderSlot,
    pub request_id: Option<u64>,
    pub actual_ns: u64,
    pub scheduled_ns: Option<u64>,
    pub deadline_ns: Option<u64>,
    pub status: Option<OrderStatus>,
    pub fill_quantity: f64,
    /// Immutable exchange matching clock, distinct from strategy delivery.
    pub exchange_event_timestamp_ns: Option<u64>,
    pub timing: Option<CancelTiming>,
}

pub fn inline_identity(value: &str) -> ArrayString<128> {
    ArrayString::from(value)
        .expect("sim_v2 timing audit identity exceeds 128 bytes; replay aborted")
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ExecutionTimingStats {
    pub enabled: bool,
    pub emitted: u64,
    pub drained: u64,
    pub queued: usize,
    pub high_water: usize,
    pub capacity: usize,
    pub overflows: u64,
}

pub struct ExecutionTimingAudit {
    rows: VecDeque<ExecutionTimingRow>,
    stats: ExecutionTimingStats,
}

impl ExecutionTimingAudit {
    pub fn new(enabled: bool) -> Self {
        Self {
            rows: VecDeque::with_capacity(if enabled {
                EXECUTION_TIMING_CAPACITY
            } else {
                0
            }),
            stats: ExecutionTimingStats {
                enabled,
                emitted: 0,
                drained: 0,
                queued: 0,
                high_water: 0,
                capacity: EXECUTION_TIMING_CAPACITY,
                overflows: 0,
            },
        }
    }
    pub fn enabled(&self) -> bool {
        self.stats.enabled
    }
    pub fn push(&mut self, row: ExecutionTimingRow) {
        if !self.stats.enabled {
            return;
        }
        if self.rows.len() == EXECUTION_TIMING_CAPACITY {
            self.stats.overflows += 1;
            panic!("sim_v2 execution timing audit capacity exceeded; replay aborted without dropping evidence");
        }
        self.rows.push_back(row);
        self.stats.emitted += 1;
        self.stats.high_water = self.stats.high_water.max(self.rows.len());
    }
    pub fn drain(&mut self) -> impl Iterator<Item = ExecutionTimingRow> + '_ {
        self.stats.drained += self.rows.len() as u64;
        self.rows.drain(..)
    }
    pub fn stats(&self) -> ExecutionTimingStats {
        ExecutionTimingStats {
            queued: self.rows.len(),
            ..self.stats
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct CancelTimingStats {
    pub mode: CancelTimingMode,
    pub processing_requested_ns: u64,
    pub processing_fraction_bps: u16,
    pub staged_requests: u64,
    pub processing_capped_requests: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fraction_conserves_rtt_without_erasing_network_and_does_not_overflow() {
        for (l1, l2) in [
            (0, 0),
            (0, 1),
            (1, 0),
            (1, 1),
            (3, 101),
            (14_334_742, 14_334_743),
            (u64::MAX / 2, u64::MAX / 2),
        ] {
            for bps in [0, 1, 2000, 5000, 9999] {
                let t = CancelTiming::fraction(0, l1, l2, bps);
                assert_eq!(t.network_l1_ns + t.processing_ns + t.network_l2_ns, l1 + l2);
                assert_eq!(t.nominal_http_ns, l1 + l2);
                assert!(!t.processing_capped);
                if l1 > 0 {
                    assert!(t.network_l1_ns > 0);
                }
                if l2 > 0 {
                    assert!(t.network_l2_ns > 0);
                }
                assert!(
                    t.nominal_arrival_ns <= t.nominal_effective_ns
                        && t.nominal_effective_ns <= t.nominal_http_ns
                );
            }
        }
        let t = CancelTiming::fraction(100, 50, 50, 2000);
        assert_eq!(
            (
                t.nominal_arrival_ns,
                t.nominal_effective_ns,
                t.nominal_http_ns
            ),
            (140, 160, 200)
        );
    }
    #[test]
    #[should_panic(expected = "below 10000")]
    fn fraction_rejects_all_processing() {
        CancelTiming::fraction(0, 50, 50, 10000);
    }

    #[test]
    fn staged_partition_conserves_rtt_and_caps_processing() {
        let t = CancelTiming::partition(1_000, 50, 50, 40);
        assert_eq!(
            (t.network_l1_ns, t.processing_ns, t.network_l2_ns),
            (30, 40, 30)
        );
        assert_eq!(
            (
                t.nominal_arrival_ns,
                t.nominal_effective_ns,
                t.nominal_http_ns
            ),
            (1030, 1070, 1100)
        );
        for l1 in 0..31 {
            for l2 in 0..31 {
                for p in [0, 1, 7, 29, 100] {
                    let t = CancelTiming::partition(1000, l1, l2, p);
                    assert_eq!(t.network_l1_ns + t.processing_ns + t.network_l2_ns, l1 + l2);
                    assert_eq!(t.nominal_http_ns, 1000 + l1 + l2);
                    assert!(
                        t.nominal_arrival_ns <= t.nominal_effective_ns
                            && t.nominal_effective_ns <= t.nominal_http_ns
                    );
                    assert_eq!(t.processing_capped, p > l1 + l2);
                }
            }
        }
    }
}
