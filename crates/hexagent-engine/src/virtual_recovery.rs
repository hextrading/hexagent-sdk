//! Offline warm-checkpoint outage protocol. This does not reconstruct an empty
//! process or claim that an actual checkpoint was captured. The in-memory
//! strategy account/order state at stop is the explicit checkpoint assumption.
//! Venue orders/wallet and server matching remain under Simulator ownership.
use crate::exchange::sim_v2::simulator::RecoveryDeliveryWatermark;
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;

const MAX_RECOVERY_JOURNAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECOVERY_WINDOWS: usize = 16_384;

#[derive(Deserialize, Clone)]
struct Window {
    instance_id: String,
    stop_ns: u64,
    restart_ns: u64,
    evidence_class: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryAction {
    Stop,
    Restart,
    ReconcileComplete,
}
#[derive(Clone, Copy)]
pub(crate) struct RecoveryEdge {
    pub when_ns: u64,
    pub owner: usize,
    pub action: RecoveryAction,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    #[default]
    Ready,
    Offline,
    Reconciling,
    Catchup,
}
#[derive(Default, Serialize)]
struct State {
    phase: Phase,
    restart_ns: u64,
    fresh_book_applied: bool,
    fresh_until_ns: u64,
    stops: u64,
    ready_transitions: u64,
    ready_at_ns: u64,
    delivery_watermark: Option<RecoveryDeliveryWatermark>,
    oracle_history_pending: bool,
    oracle_history_rows_applied: u64,
    oracle_history_queries_completed: u64,
    oracle_history_queries_failed: u64,
    oracle_history_cutoff_ns: u64,
    oracle_history_applied_ns: u64,
    oracle_history_required_prices_ready: bool,
}

#[derive(Serialize)]
pub(crate) struct OracleHistoryQueryAudit {
    pub owner: usize,
    pub cutoff_ns: u64,
    pub applied_ns: u64,
    pub input_rows: usize,
    pub applied_rows: usize,
    pub excluded_unavailable_rows: usize,
    pub max_observation_ns: u64,
    pub max_receive_ns: u64,
    pub boundary_prices_restored: usize,
    pub required_prices_ready: bool,
}
pub(crate) struct RecoveryReplay {
    edges: Vec<RecoveryEdge>,
    cursor: usize,
    states: Vec<State>,
    oracle_queries: Vec<OracleHistoryQueryAudit>,
}
impl RecoveryReplay {
    pub fn from_path(
        path: &str,
        owners: &HashMap<String, usize>,
        reconcile_delay_ns: u64,
        start_ns: u64,
        end_ns: u64,
    ) -> Result<Self> {
        if path.trim().is_empty() {
            return Ok(Self {
                edges: Vec::new(),
                cursor: 0,
                states: (0..owners.len()).map(|_| State::default()).collect(),
                oracle_queries: Vec::new(),
            });
        }
        let file =
            std::fs::File::open(path).with_context(|| format!("open recovery journal {path}"))?;
        ensure!(
            file.metadata()?.len() <= MAX_RECOVERY_JOURNAL_BYTES as u64,
            "recovery journal exceeds {} byte capacity",
            MAX_RECOVERY_JOURNAL_BYTES
        );
        let mut text = String::new();
        file.take(MAX_RECOVERY_JOURNAL_BYTES as u64 + 1)
            .read_to_string(&mut text)?;
        ensure!(
            text.len() <= MAX_RECOVERY_JOURNAL_BYTES,
            "recovery journal grew beyond {} byte capacity",
            MAX_RECOVERY_JOURNAL_BYTES
        );
        Self::from_text_bounded(&text, owners, reconcile_delay_ns, start_ns, end_ns)
    }
    #[cfg(test)]
    fn from_text(
        text: &str,
        owners: &HashMap<String, usize>,
        reconcile_delay_ns: u64,
    ) -> Result<Self> {
        Self::from_text_bounded(text, owners, reconcile_delay_ns, 0, u64::MAX)
    }
    fn from_text_bounded(
        text: &str,
        owners: &HashMap<String, usize>,
        reconcile_delay_ns: u64,
        start_ns: u64,
        end_ns: u64,
    ) -> Result<Self> {
        ensure!(
            text.len() <= MAX_RECOVERY_JOURNAL_BYTES,
            "recovery journal exceeds {} byte capacity",
            MAX_RECOVERY_JOURNAL_BYTES
        );
        let mut windows = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .take(MAX_RECOVERY_WINDOWS + 1)
            .map(serde_json::from_str::<Window>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            windows.len() <= MAX_RECOVERY_WINDOWS,
            "recovery journal exceeds {} window capacity",
            MAX_RECOVERY_WINDOWS
        );
        windows.sort_by_key(|w| (w.stop_ns, w.restart_ns));
        let mut previous_end = HashMap::new();
        let mut edges = Vec::with_capacity(windows.len() * 3);
        for window in windows {
            if window.restart_ns.saturating_add(reconcile_delay_ns) < start_ns
                || window.stop_ns > end_ns
            {
                continue;
            }
            let owner = *owners
                .get(&window.instance_id)
                .with_context(|| format!("unknown recovery owner {}", window.instance_id))?;
            ensure!(
                window.stop_ns < window.restart_ns,
                "recovery stop must precede restart"
            );
            ensure!(
                window.evidence_class == "observed_log_boundary",
                "recovery journal must identify observed_log_boundary evidence"
            );
            ensure!(
                previous_end
                    .get(&owner)
                    .is_none_or(|end| *end <= window.stop_ns),
                "overlapping recovery windows for {}",
                window.instance_id
            );
            let reconciled = window
                .restart_ns
                .checked_add(reconcile_delay_ns)
                .context("recovery reconcile time overflow")?;
            previous_end.insert(owner, reconciled);
            edges.extend([
                RecoveryEdge {
                    when_ns: window.stop_ns.max(start_ns),
                    owner,
                    action: RecoveryAction::Stop,
                },
                RecoveryEdge {
                    when_ns: window.restart_ns.max(start_ns),
                    owner,
                    action: RecoveryAction::Restart,
                },
                RecoveryEdge {
                    when_ns: reconciled,
                    owner,
                    action: RecoveryAction::ReconcileComplete,
                },
            ]);
        }
        edges.sort_by_key(|e| {
            (
                e.when_ns,
                e.owner,
                match e.action {
                    RecoveryAction::Stop => 0,
                    RecoveryAction::Restart => 1,
                    RecoveryAction::ReconcileComplete => 2,
                },
            )
        });
        Ok(Self {
            edges,
            cursor: 0,
            states: (0..owners.len()).map(|_| State::default()).collect(),
            oracle_queries: Vec::with_capacity(MAX_RECOVERY_WINDOWS),
        })
    }
    pub fn peek_when(&self) -> Option<u64> {
        self.edges.get(self.cursor).map(|e| e.when_ns)
    }
    pub fn peek_action(&self) -> Option<RecoveryAction> {
        self.edges.get(self.cursor).map(|e| e.action)
    }
    pub fn capture_watermark(&mut self, owner: usize, watermark: RecoveryDeliveryWatermark) {
        self.states[owner].delivery_watermark = Some(watermark);
    }
    pub fn watermark(&self, owner: usize) -> Option<&RecoveryDeliveryWatermark> {
        (self.states[owner].phase == Phase::Catchup)
            .then_some(self.states[owner].delivery_watermark.as_ref())
            .flatten()
    }
    pub fn step(&mut self, now_ns: u64) -> Option<RecoveryEdge> {
        let edge = *self
            .edges
            .get(self.cursor)
            .filter(|e| e.when_ns <= now_ns)?;
        self.cursor += 1;
        let state = &mut self.states[edge.owner];
        match edge.action {
            RecoveryAction::Stop => {
                state.phase = Phase::Offline;
                state.fresh_book_applied = false;
                state.stops += 1;
                state.delivery_watermark = None;
                state.oracle_history_pending = true;
                state.oracle_history_required_prices_ready = false;
            }
            RecoveryAction::Restart => {
                state.phase = Phase::Reconciling;
                state.restart_ns = edge.when_ns;
                state.fresh_book_applied = false;
            }
            RecoveryAction::ReconcileComplete => state.phase = Phase::Catchup,
        }
        Some(edge)
    }
    pub fn collecting_oracle_history(&self, owner: usize) -> bool {
        matches!(
            self.states[owner].phase,
            Phase::Offline | Phase::Reconciling
        )
    }
    pub fn complete_oracle_history(&mut self, query: OracleHistoryQueryAudit) {
        assert!(
            self.oracle_queries.len() < MAX_RECOVERY_WINDOWS,
            "oracle query audit capacity exceeded"
        );
        let state = &mut self.states[query.owner];
        state.oracle_history_cutoff_ns = query.cutoff_ns;
        state.oracle_history_applied_ns = query.applied_ns;
        state.oracle_history_required_prices_ready = query.required_prices_ready;
        if query.required_prices_ready {
            state.oracle_history_pending = false;
            state.oracle_history_rows_applied += query.applied_rows as u64;
            state.oracle_history_queries_completed += 1;
        } else {
            state.oracle_history_queries_failed += 1;
        }
        self.oracle_queries.push(query);
    }
    pub fn discard_market(&self, owner: usize) -> bool {
        self.states[owner].phase == Phase::Offline
    }
    pub fn quote_allowed(&self, owner: usize) -> bool {
        self.states[owner].phase == Phase::Ready
    }
    pub fn observe_fresh_book(
        &mut self,
        owner: usize,
        received_ns: u64,
        source_ns: u64,
        applied_ns: u64,
        valid: bool,
        max_age_ns: u64,
    ) {
        let state = &mut self.states[owner];
        if state.phase == Phase::Catchup {
            state.fresh_book_applied = false;
        }
        if state.phase == Phase::Catchup
            && valid
            && received_ns >= state.restart_ns
            && source_ns != 0
            && applied_ns.saturating_sub(source_ns) <= max_age_ns
        {
            state.fresh_book_applied = true;
            state.fresh_until_ns = source_ns.saturating_add(max_age_ns);
        }
    }
    pub fn try_ready(&mut self, owner: usize, private_drained: bool, now_ns: u64) -> bool {
        let state = &mut self.states[owner];
        if state.phase == Phase::Catchup
            && !state.oracle_history_pending
            && state.fresh_book_applied
            && now_ns <= state.fresh_until_ns
            && private_drained
        {
            state.phase = Phase::Ready;
            state.ready_transitions += 1;
            state.ready_at_ns = now_ns;
            return true;
        }
        false
    }
    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({ "schema": "sim_warm_recovery_v3", "checkpoint": "modeled in-memory account, orders, reservations at stop; not a recorded checkpoint or cold start", "query_delay": "modeled configuration, not observed RTT", "venue_state": "server matching and wallet continue while owner is offline", "private_recovery": "bounded original-identity backlog and fixed pre-query delivery/request/trade-id watermark; conservative delivery barrier, not an authoritative venue snapshot query", "oracle_history": "modeled archive query at restart completion; original receive and observation must both be at/before cutoff; historical boundary inputs only, never current market callbacks", "oracle_queries": self.oracle_queries, "edges_applied": self.cursor, "edges_total": self.edges.len(), "owners": self.states })
    }
}

/// Recorder-side oracle history remains available while the strategy process is
/// down. This bounded journal is queried at restart; it never drives callbacks
/// during the outage and never coalesces away boundary reports.
const MAX_RECOVERY_ORACLE_REPORTS: usize = 4096;
pub(crate) struct RecoveryOracleHistory {
    reports: Vec<Vec<crate::types::SpotPrice>>,
    captured: Vec<u64>,
    high_water: Vec<usize>,
}
impl RecoveryOracleHistory {
    pub fn new(count: usize) -> Self {
        Self {
            reports: (0..count)
                .map(|_| Vec::with_capacity(MAX_RECOVERY_ORACLE_REPORTS))
                .collect(),
            captured: vec![0; count],
            high_water: vec![0; count],
        }
    }
    pub fn observe(&mut self, owner: usize, report: &crate::types::SpotPrice) -> Result<()> {
        if !matches!(
            report.source.as_str(),
            "chainlink" | "chainlink_stream" | "rtds_chainlink"
        ) {
            return Ok(());
        }
        ensure!(self.reports[owner].len() < MAX_RECOVERY_ORACLE_REPORTS,
            "virtual owner {owner} oracle history capacity exceeded; recovery fails closed without losing boundary reports");
        self.reports[owner].push(report.clone());
        self.captured[owner] += 1;
        self.high_water[owner] = self.high_water[owner].max(self.reports[owner].len());
        Ok(())
    }
    pub fn reports(&self, owner: usize) -> &[crate::types::SpotPrice] {
        &self.reports[owner]
    }
    pub fn causal_reports(&self, owner: usize, cutoff_ns: u64) -> Vec<crate::types::SpotPrice> {
        self.reports[owner]
            .iter()
            .filter(|report| {
                report.timestamp_ns > 0
                    && report.timestamp_ns <= cutoff_ns
                    && report.local_timestamp_ns > 0
                    && report.local_timestamp_ns <= cutoff_ns
            })
            .cloned()
            .collect()
    }
    pub fn clear_applied(&mut self, owner: usize) {
        self.reports[owner].clear();
    }
    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({"capacity_per_owner": MAX_RECOVERY_ORACLE_REPORTS, "captured": self.captured,
            "high_water": self.high_water, "pending_per_owner": self.reports.iter().map(Vec::len).collect::<Vec<_>>(),
            "overflow_policy": "abort before losing any oracle boundary", "coalescing": false})
    }
}

/// The checkpoint already contains metadata whose callback completed. Retain
/// only not-yet-applied immutable metadata, including records received offline.
/// Historical archives need not contain EventEnd: completed records are removed
/// immediately instead of accumulating one entry for every past instrument.
const MAX_PENDING_METADATA_PER_OWNER: usize = 64;
pub(crate) struct RecoveryMetadata {
    owners: Vec<Vec<std::sync::Arc<crate::types::MarketEvent>>>,
    high_water: Vec<usize>,
}
impl RecoveryMetadata {
    pub fn new(count: usize) -> Self {
        Self {
            owners: (0..count)
                .map(|_| Vec::with_capacity(MAX_PENDING_METADATA_PER_OWNER))
                .collect(),
            high_water: vec![0; count],
        }
    }
    pub fn observe(
        &mut self,
        owner: usize,
        event: &std::sync::Arc<crate::types::MarketEvent>,
    ) -> Result<()> {
        use crate::types::{Instrument, MarketEvent};
        let MarketEvent::Instrument(incoming) = event.as_ref() else {
            return Ok(());
        };
        let same_instrument =
            |previous: &std::sync::Arc<MarketEvent>| match (previous.as_ref(), incoming) {
                (
                    MarketEvent::Instrument(Instrument::BinaryOption(a)),
                    Instrument::BinaryOption(b),
                ) => a.exchange == b.exchange && a.condition_id == b.condition_id,
                (MarketEvent::Instrument(Instrument::Spot(a)), Instrument::Spot(b)) => {
                    a.exchange == b.exchange && a.symbol == b.symbol
                }
                _ => false,
            };
        let pending = &mut self.owners[owner];
        if let Some(previous) = pending.iter_mut().find(|entry| same_instrument(entry)) {
            *previous = std::sync::Arc::clone(event);
        } else {
            ensure!(
                pending.len() < MAX_PENDING_METADATA_PER_OWNER,
                "virtual owner {owner} pending metadata capacity exceeded; recovery fails closed"
            );
            pending.push(std::sync::Arc::clone(event));
            self.high_water[owner] = self.high_water[owner].max(pending.len());
        }
        Ok(())
    }
    pub fn applied(&mut self, owner: usize, event: &std::sync::Arc<crate::types::MarketEvent>) {
        // A superseded callback must not acknowledge a newer metadata version.
        self.owners[owner].retain(|pending| !std::sync::Arc::ptr_eq(pending, event));
    }
    pub fn retire(&mut self, tokens: &[String]) {
        use crate::types::{Instrument, MarketEvent};
        for pending in &mut self.owners {
            pending.retain(|entry| !matches!(entry.as_ref(), MarketEvent::Instrument(Instrument::BinaryOption(bo)) if bo.clob_token_ids.iter().any(|id| tokens.contains(id))));
        }
    }
    pub fn pending(&self, owner: usize) -> &[std::sync::Arc<crate::types::MarketEvent>] {
        &self.owners[owner]
    }
    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({"capacity_per_owner": MAX_PENDING_METADATA_PER_OWNER, "high_water": self.high_water,
            "pending_per_owner": self.owners.iter().map(Vec::len).collect::<Vec<_>>(),
            "overflow_policy": "abort before metadata loss", "retention": "unapplied metadata only; applied metadata lives in the warm strategy checkpoint"})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn replay() -> RecoveryReplay {
        let owners = HashMap::from([("btc01".to_string(), 0)]);
        RecoveryReplay::from_text(r#"{"instance_id":"btc01","stop_ns":10,"restart_ns":20,"evidence_class":"observed_log_boundary"}"#, &owners, 5).unwrap()
    }
    #[test]
    fn oracle_archive_is_lossless_bounded_owner_local_and_query_causal() {
        let mut history = RecoveryOracleHistory::new(2);
        let row = |observation, receive| crate::types::SpotPrice {
            source: "chainlink_stream".into(),
            symbol: "btc/usd/twap/60".into(),
            price: observation as f64,
            timestamp_ns: observation,
            local_timestamp_ns: receive,
        };
        for ts in 1..=120 {
            history.observe(0, &row(ts, ts + 1)).unwrap();
        }
        history.observe(0, &row(200, 100)).unwrap(); // future observation
        history.observe(0, &row(100, 200)).unwrap(); // not received at query cutoff
        assert_eq!(history.reports(0).len(), 122);
        assert_eq!(history.causal_reports(0, 121).len(), 120);
        assert_eq!(
            history.causal_reports(0, 121)[59].price,
            60.0,
            "do not coalesce a five-minute boundary"
        );
        assert!(history.reports(1).is_empty());
        history.clear_applied(0);
        for _ in 0..MAX_RECOVERY_ORACLE_REPORTS {
            history.observe(0, &row(1, 1)).unwrap();
        }
        assert!(history.observe(0, &row(2, 2)).is_err());
    }

    #[test]
    fn fresh_applied_book_and_complete_private_catchup_both_required() {
        let mut r = replay();
        assert!(r.quote_allowed(0));
        assert_eq!(r.step(10).unwrap().action, RecoveryAction::Stop);
        assert!(r.discard_market(0));
        r.step(20);
        r.step(25);
        r.observe_fresh_book(0, 25, 25, 25, true, 100);
        assert!(
            !r.try_ready(0, true, 25),
            "oracle history must be applied even when metadata/private are ready"
        );
        r.complete_oracle_history(OracleHistoryQueryAudit {
            owner: 0,
            cutoff_ns: 25,
            applied_ns: 25,
            input_rows: 3,
            applied_rows: 3,
            excluded_unavailable_rows: 0,
            max_observation_ns: 24,
            max_receive_ns: 24,
            boundary_prices_restored: 2,
            required_prices_ready: true,
        });
        r.observe_fresh_book(0, 19, 19, 25, true, 100);
        assert!(!r.try_ready(0, true, 25)); // old receive
        r.observe_fresh_book(0, 26, 1, 26, true, 10);
        assert!(!r.try_ready(0, true, 26)); // stale source
        r.observe_fresh_book(0, 27, 27, 27, false, 10);
        assert!(!r.try_ready(0, true, 27)); // invalid book
        r.observe_fresh_book(0, 28, 28, 28, true, 10);
        assert!(!r.try_ready(0, false, 28)); // undrained private lane
        assert!(!r.try_ready(0, true, 40)); // backlog drain after book expired
        r.observe_fresh_book(0, 41, 41, 41, true, 10);
        assert!(r.try_ready(0, true, 42));
        assert!(r.quote_allowed(0));
        assert!(!r.try_ready(0, true, 30)); // idempotent transition
    }
    #[test]
    fn recovery_journal_has_explicit_byte_and_window_capacities() {
        let owners = HashMap::from([("btc01".to_string(), 0)]);
        assert!(
            RecoveryReplay::from_text(&" ".repeat(MAX_RECOVERY_JOURNAL_BYTES + 1), &owners, 0)
                .err()
                .unwrap()
                .to_string()
                .contains("byte capacity")
        );
        let row = "{\"instance_id\":\"btc01\",\"stop_ns\":10,\"restart_ns\":20,\"evidence_class\":\"observed_log_boundary\"}\n";
        assert!(
            RecoveryReplay::from_text(&row.repeat(MAX_RECOVERY_WINDOWS + 1), &owners, 0)
                .err()
                .unwrap()
                .to_string()
                .contains("window capacity")
        );
    }

    #[test]
    fn rejects_unknown_unobserved_or_overlapping_windows() {
        let owners = HashMap::from([("btc01".to_string(), 0)]);
        for line in [
            r#"{"instance_id":"wrong","stop_ns":10,"restart_ns":20,"evidence_class":"observed_log_boundary"}"#,
            r#"{"instance_id":"btc01","stop_ns":10,"restart_ns":20,"evidence_class":"inferred_ready"}"#,
            r#"{"instance_id":"btc01","stop_ns":20,"restart_ns":10,"evidence_class":"observed_log_boundary"}"#,
        ] {
            assert!(RecoveryReplay::from_text(line, &owners, 5).is_err());
        }
        let line = r#"{"instance_id":"btc01","stop_ns":10,"restart_ns":20,"evidence_class":"observed_log_boundary"}"#;
        assert!(RecoveryReplay::from_text(&format!("{line}\n{line}"), &owners, 5).is_err());
    }
}
