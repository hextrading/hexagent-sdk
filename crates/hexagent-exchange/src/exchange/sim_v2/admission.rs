//! Offline replay of *observed* new-placement admission protection.
//!
//! The owning simulator loads this immutable table before entering its event
//! loop. `blocked` only inspects the supplied event time: it never reads a file,
//! grows the heap, locks, logs, counts attempts, or advances a random stream.
//! Random-access lookup deliberately remains correct after a replay seek or a
//! timestamp regression; a monotonic cursor is not required.
//!
//! Integration contract: consult this table once at the admission point of a
//! **new place request**, before RTT sampling/exchange acceptance. `true` means
//! the caller produces its normal owner-routed GateClosed rejection. Cancel,
//! reconciliation, existing-order matching, private fills and lifecycle updates
//! must continue. Do not skip a market event or stop draining those lanes.
//!
//! The table contains recorded enter/extension evidence, not PnL, outcomes or
//! observed trades. It is a diagnostic replay of a lower bound on protection:
//! unlogged gate extensions may be missing. It is not a calibrated gate model
//! for counterfactual strategies. Each simulator instance owns any actual
//! rejected-request counter; repeated `blocked` queries are not unique attempts.

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

/// Defensive startup limits, unrelated to the allocation-free lookup path.
pub const MAX_SOURCE_INTERVALS: usize = 262_144;
const MAX_CSV_LINE_BYTES: usize = 4_096;
const HEADER: &str = "start_ns,end_ns,duration_seconds";

/// Half-open interval: the gate is closed at `start_ns`, open at `end_ns`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GateInterval {
    pub start_ns: u64,
    pub end_ns: u64,
}

/// Sorted, disjoint, non-touching intervals; constructed entirely at startup.
#[derive(Debug, Default)]
pub struct ObservedAdmissionReplay {
    intervals: Box<[GateInterval]>,
    source_interval_count: usize,
    total_blocked_ns: u64,
}

impl ObservedAdmissionReplay {
    /// Load a strict three-column CSV. Invalid input returns an error; callers
    /// must not silently replace a failed configured replay with an empty gate.
    pub fn from_path(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("admission replay {}: {error}", path.display()),
            )
        })?;
        Self::from_reader(BufReader::new(file))
    }

    /// Reader variant for deterministic fixtures. Input must have nondecreasing
    /// starts; overlap, duplicate and adjacent intervals are merged at startup.
    /// `duration_seconds` is checked but integer nanoseconds remain authoritative.
    pub fn from_reader(mut reader: impl BufRead) -> io::Result<Self> {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Err(invalid(1, "missing CSV header"));
        }
        if line.len() > MAX_CSV_LINE_BYTES {
            return Err(invalid(1, "CSV line exceeds byte limit"));
        }
        let header = line.trim().trim_start_matches('\u{feff}');
        if header != HEADER {
            return Err(invalid(1, "expected start_ns,end_ns,duration_seconds"));
        }
        let mut intervals: Vec<GateInterval> = Vec::new();
        let mut source_interval_count = 0;
        let mut previous_start = None;
        let mut line_number = 1;
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            line_number += 1;
            if line.len() > MAX_CSV_LINE_BYTES {
                return Err(invalid(line_number, "CSV line exceeds byte limit"));
            }
            let text = line.trim();
            if text.is_empty() {
                continue;
            }
            if source_interval_count == MAX_SOURCE_INTERVALS {
                return Err(invalid(line_number, "source interval capacity exceeded"));
            }
            let mut fields = text.split(',');
            let start = fields.next().unwrap_or_default().trim();
            let end = fields
                .next()
                .ok_or_else(|| invalid(line_number, "missing end_ns"))?
                .trim();
            let duration = fields
                .next()
                .ok_or_else(|| invalid(line_number, "missing duration_seconds"))?
                .trim();
            if fields.next().is_some() {
                return Err(invalid(line_number, "expected exactly three CSV fields"));
            }
            let start_ns = start
                .parse::<u64>()
                .map_err(|_| invalid(line_number, "start_ns is not a u64 integer"))?;
            let end_ns = end
                .parse::<u64>()
                .map_err(|_| invalid(line_number, "end_ns is not a u64 integer"))?;
            if end_ns <= start_ns {
                return Err(invalid(
                    line_number,
                    "interval must satisfy start_ns < end_ns",
                ));
            }
            if previous_start.is_some_and(|previous| start_ns < previous) {
                return Err(invalid(
                    line_number,
                    "interval starts are not nondecreasing",
                ));
            }
            let seconds = duration
                .parse::<f64>()
                .map_err(|_| invalid(line_number, "duration_seconds is not numeric"))?;
            if !seconds.is_finite() || seconds <= 0.0 {
                return Err(invalid(
                    line_number,
                    "duration_seconds must be finite and positive",
                ));
            }
            let expected_seconds = (end_ns - start_ns) as f64 / 1_000_000_000.0;
            // Allow one nanosecond and ordinary f64 conversion error, but not
            // millisecond/second unit mistakes. Endpoints are never reconstructed
            // from floating point seconds.
            let tolerance = 1e-9_f64.max(8.0 * f64::EPSILON * expected_seconds);
            if (seconds - expected_seconds).abs() > tolerance {
                return Err(invalid(
                    line_number,
                    "duration_seconds disagrees with integer endpoints",
                ));
            }
            previous_start = Some(start_ns);
            source_interval_count += 1;
            match intervals.last_mut() {
                Some(last) if start_ns <= last.end_ns => {
                    last.end_ns = last.end_ns.max(end_ns);
                }
                _ => intervals.push(GateInterval { start_ns, end_ns }),
            }
        }
        let total_blocked_ns = intervals.iter().try_fold(0_u64, |total, interval| {
            total
                .checked_add(interval.end_ns - interval.start_ns)
                .ok_or_else(|| invalid(line_number, "merged duration overflow"))
        })?;
        Ok(Self {
            intervals: intervals.into_boxed_slice(),
            source_interval_count,
            total_blocked_ns,
        })
    }

    /// Pure O(log N) membership test. No mutable cursor or attempt telemetry.
    #[inline]
    pub fn blocked(&self, now_ns: u64) -> bool {
        let index = self
            .intervals
            .partition_point(|interval| interval.start_ns <= now_ns);
        index > 0 && now_ns < self.intervals[index - 1].end_ns
    }

    pub fn interval_count(&self) -> usize {
        self.intervals.len()
    }

    pub fn source_interval_count(&self) -> usize {
        self.source_interval_count
    }

    pub fn total_blocked_ns(&self) -> u64 {
        self.total_blocked_ns
    }

    /// Immutable canonical intervals for startup/audit reporting only.
    pub fn intervals(&self) -> &[GateInterval] {
        &self.intervals
    }
}

fn invalid(line: usize, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("admission CSV line {line}: {reason}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn table(intervals: &[(u64, u64)]) -> ObservedAdmissionReplay {
        let mut csv = format!("{HEADER}\n");
        for &(start, end) in intervals {
            csv.push_str(&format!(
                "{start},{end},{:.12}\n",
                (end - start) as f64 / 1e9
            ));
        }
        ObservedAdmissionReplay::from_reader(Cursor::new(csv)).unwrap()
    }

    #[test]
    fn membership_is_start_inclusive_end_exclusive() {
        let gate = table(&[(10, 20), (30, 40)]);
        for (now, expected) in [
            (0, false),
            (9, false),
            (10, true),
            (19, true),
            (20, false),
            (29, false),
            (30, true),
            (39, true),
            (40, false),
            (u64::MAX, false),
        ] {
            assert_eq!(gate.blocked(now), expected, "now={now}");
        }
        assert_eq!(gate.total_blocked_ns(), 20);
    }

    #[test]
    fn duplicates_overlap_nested_and_adjacent_rows_merge() {
        let gate = table(&[(10, 20), (10, 20), (12, 14), (18, 25), (25, 30), (40, 50)]);
        assert_eq!(gate.source_interval_count(), 6);
        assert_eq!(
            gate.intervals(),
            &[
                GateInterval {
                    start_ns: 10,
                    end_ns: 30
                },
                GateInterval {
                    start_ns: 40,
                    end_ns: 50
                }
            ]
        );
        assert_eq!(gate.interval_count(), 2);
        assert_eq!(gate.total_blocked_ns(), 30);
    }

    #[test]
    fn repeated_queries_backward_seek_and_replay_are_idempotent() {
        let gate = table(&[(10, 20), (30, 40)]);
        for _ in 0..5 {
            assert!(!gate.blocked(100));
            assert!(gate.blocked(15));
            assert!(gate.blocked(15));
            assert!(gate.blocked(30));
            assert!(!gate.blocked(20));
        }
        assert_eq!(gate.source_interval_count(), 2);
        assert_eq!(gate.total_blocked_ns(), 20);
    }

    #[test]
    fn independent_instance_tables_do_not_share_mutable_state() {
        let a = table(&[(10, 20)]);
        let b = table(&[(30, 40)]);
        assert!(a.blocked(15));
        assert!(!b.blocked(15));
        assert!(!a.blocked(35));
        assert!(b.blocked(35));
    }

    #[test]
    fn header_only_and_default_mean_no_observed_intervals() {
        for gate in [ObservedAdmissionReplay::default(), table(&[])] {
            assert_eq!(gate.interval_count(), 0);
            assert_eq!(gate.total_blocked_ns(), 0);
            assert!(!gate.blocked(0));
            assert!(!gate.blocked(u64::MAX));
        }
    }

    #[test]
    fn crlf_bom_blank_lines_and_final_line_without_newline_are_supported() {
        let csv = format!("\u{feff}{HEADER}\r\n\r\n10,20,0.00000001");
        let gate = ObservedAdmissionReplay::from_reader(Cursor::new(csv)).unwrap();
        assert!(gate.blocked(10));
        assert!(!gate.blocked(20));
    }

    #[test]
    fn rejects_malformed_nonfinite_misordered_or_inconsistent_rows() {
        for row in [
            "",
            "bad,2,1",
            "-1,2,1",
            "1.5,2,1",
            "18446744073709551616,2,1",
            "10,10,0",
            "20,10,1",
            "10,20,NaN",
            "10,20,inf",
            "10,20,-1",
            "10,20,0",
            "10,20,1",
            "10,20",
            "10,20,0.00000001,extra",
            "10,20,0.00000001\n9,30,0.000000021",
        ] {
            // Empty input is missing a header; other fixtures supply it.
            let csv = if row.is_empty() {
                String::new()
            } else {
                format!("{HEADER}\n{row}\n")
            };
            let error = ObservedAdmissionReplay::from_reader(Cursor::new(csv)).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "row={row}");
        }
        for header in [
            "start,end,duration_seconds",
            "start_ns,end_ns",
            "end_ns,start_ns,duration_seconds",
            "start_ns,end_ns,pnl",
        ] {
            assert!(
                ObservedAdmissionReplay::from_reader(Cursor::new(format!("{header}\n"))).is_err()
            );
        }
    }

    #[test]
    fn validates_source_order_even_inside_one_merged_interval() {
        let csv = format!("{HEADER}\n10,100,0.00000009\n30,40,0.00000001\n20,25,0.000000005\n");
        assert!(ObservedAdmissionReplay::from_reader(Cursor::new(csv)).is_err());
    }

    #[test]
    fn integer_endpoints_preserve_one_nanosecond_precision_at_epoch_scale() {
        let start = 1_789_307_472_261_000_000;
        let gate = table(&[(start, start + 1), (u64::MAX - 1, u64::MAX)]);
        assert!(!gate.blocked(start - 1));
        assert!(gate.blocked(start));
        assert!(!gate.blocked(start + 1));
        assert!(gate.blocked(u64::MAX - 1));
        assert!(!gate.blocked(u64::MAX));
        assert_eq!(gate.total_blocked_ns(), 2);
    }

    #[test]
    fn startup_capacity_and_line_limits_fail_instead_of_truncating() {
        let mut csv = format!("{HEADER}\n");
        // Duplicates still count against input capacity even when unioned into
        // a single interval; silent truncation would conceal bad recordings.
        for _ in 0..=MAX_SOURCE_INTERVALS {
            csv.push_str("10,20,0.00000001\n");
        }
        assert!(ObservedAdmissionReplay::from_reader(Cursor::new(csv)).is_err());
        let csv = format!("{HEADER}\n{}\n", "0".repeat(MAX_CSV_LINE_BYTES + 1));
        assert!(ObservedAdmissionReplay::from_reader(Cursor::new(csv)).is_err());
    }
}
