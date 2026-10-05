//! Explicit offline decision clocks. No market payloads or outcomes are read
//! from this file. The backtest loop applies all ingress at or before each
//! decision, then invokes the named owner with its original trigger timestamp.
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use std::{collections::HashMap, io::BufRead};

const MAX_DECISIONS: usize = 2_000_000;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    iid: String,
    callback_ns: u64,
    decision_ns: u64,
    event_start_ns: u64,
}

#[derive(Clone, Copy)]
pub(super) struct Decision {
    pub owner: usize,
    pub callback_ns: u64,
    pub decision_ns: u64,
}

pub(super) struct DecisionReplay {
    rows: Vec<Decision>,
    cursor: usize,
    owners: Vec<bool>,
}

impl DecisionReplay {
    pub fn load(path: &str, owners: &HashMap<String, usize>, start: u64, end: u64) -> Result<Self> {
        if path.is_empty() {
            return Ok(Self { rows: Vec::new(), cursor: 0, owners: vec![false; owners.len()] });
        }
        Self::read(std::io::BufReader::new(std::fs::File::open(path)?), owners, start, end)
    }

    fn read(reader: impl BufRead, owners: &HashMap<String, usize>, start: u64, end: u64) -> Result<Self> {
        Self::read_bounded(reader, owners, start, end, MAX_DECISIONS)
    }

    fn read_bounded(reader: impl BufRead, owners: &HashMap<String, usize>, start: u64, end: u64, capacity: usize) -> Result<Self> {
        let mut replay = Self { rows: Vec::new(), cursor: 0, owners: vec![false; owners.len()] };
        let mut last = vec![None; owners.len()];
        for (line, row) in reader.lines().enumerate() {
            ensure!(replay.rows.len() < capacity, "decision replay capacity exceeded");
            let r: Input = serde_json::from_str(&row?).with_context(|| format!("decision line {}", line + 1))?;
            let owner = *owners.get(&r.iid).context("unknown decision owner")?;
            ensure!(r.callback_ns <= r.decision_ns && start <= r.decision_ns && r.decision_ns < end,
                "decision outside replay/causal clock bounds");
            ensure!(r.event_start_ns <= r.decision_ns && r.decision_ns - r.event_start_ns < 300_000_000_000,
                "decision outside declared five-minute event");
            ensure!(last[owner].is_none_or(|t| t < r.decision_ns), "duplicate/regressing owner decision");
            last[owner] = Some(r.decision_ns);
            replay.owners[owner] = true;
            replay.rows.push(Decision { owner, callback_ns: r.callback_ns, decision_ns: r.decision_ns });
        }
        ensure!(!replay.rows.is_empty(), "empty decision replay");
        replay.rows.sort_by_key(|r| (r.decision_ns, r.owner));
        Ok(replay)
    }
    pub fn peek(&self) -> u64 { self.rows.get(self.cursor).map_or(u64::MAX, |r| r.decision_ns) }
    pub fn pop(&mut self) -> Decision { let r = self.rows[self.cursor]; self.cursor += 1; r }
    pub fn owns(&self, owner: usize) -> bool { self.owners[owner] }
    pub fn len(&self) -> usize { self.rows.len() }
    pub fn completed(&self) -> usize { self.cursor }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn owners() -> HashMap<String, usize> { HashMap::from([("a".into(), 0), ("b".into(), 1)]) }
    fn line(iid: &str, decision: u64, callback: u64) -> String {
        format!("{{\"iid\":\"{iid}\",\"callback_ns\":{callback},\"decision_ns\":{decision},\"event_start_ns\":0}}\n")
    }
    #[test]
    fn sorts_across_owners_preserving_identity_and_trigger_clock() {
        let text = line("b", 12, 3) + &line("a", 10, 5) + &line("a", 12, 6);
        let mut r = DecisionReplay::read(text.as_bytes(), &owners(), 0, 20).unwrap();
        assert_eq!(r.peek(), 10); let a = r.pop(); assert_eq!((a.owner, a.callback_ns), (0, 5));
        assert_eq!(r.pop().owner, 0); assert_eq!(r.pop().owner, 1);
        assert_eq!(r.peek(), u64::MAX); assert_eq!(r.completed(), r.len());
    }
    #[test]
    fn capacity_overflow_and_event_bounds_fail_without_truncation() {
        let text = line("a", 10, 5) + &line("b", 12, 6);
        assert!(DecisionReplay::read_bounded(text.as_bytes(), &owners(), 0, 20, 1).is_err());
        assert!(DecisionReplay::read_bounded(text.as_bytes(), &owners(), 0, 20, 2).is_ok());
        let outside = line("a", 300_000_000_000, 1);
        assert!(DecisionReplay::read(outside.as_bytes(), &owners(), 0, u64::MAX).is_err());
        let empty = DecisionReplay::load("", &owners(), 0, 20).unwrap();
        assert_eq!(empty.peek(), u64::MAX);
        assert!(!empty.owns(0) && !empty.owns(1));
    }

    #[test]
    fn rejects_ambiguous_unknown_and_future_decisions() {
        for s in [line("a", 10, 5) + &line("a", 10, 6), line("a", 10, 5) + &line("a", 9, 6),
            line("c", 10, 5), line("a", 10, 11), line("a", 20, 5), String::new()] {
            assert!(DecisionReplay::read(s.as_bytes(), &owners(), 0, 20).is_err(), "{s}");
        }
    }
}
