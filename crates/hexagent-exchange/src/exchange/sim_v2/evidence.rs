//! Offline evidence, distinct from matching assumptions. All mutable state is
//! owned by one simulator. Historical normalized depth rows have no venue
//! sequence/session; replay ordinals, trades and elapsed time cannot fill that
//! gap. The causal state certifies only an observed watermark. Separately, a
//! startup-validated archive certificate can cover a historical source interval;
//! its scope is retrospective archive completeness, never live knowledge.
//! Certificates carry no prices and no order result is an input to this module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;

pub const MAX_EVIDENCE_ROWS: usize = 262_144;
const MAX_LINE_BYTES: usize = 16_384;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookContinuityMode {
    #[default]
    LegacyAge,
    SnapshotHoldEstimated,
    RequireVerified,
}

impl FromStr for BookContinuityMode {
    type Err = String;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "" | "legacy_age" => Ok(Self::LegacyAge),
            "snapshot_hold_estimated" => Ok(Self::SnapshotHoldEstimated),
            "require_verified" => Ok(Self::RequireVerified),
            _ => Err(format!("invalid book continuity mode {value}; expected legacy_age, snapshot_hold_estimated, or require_verified")),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityStatus {
    #[default]
    Unproven,
    Verified,
    Gap,
    Reconnect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityKind {
    Snapshot,
    SequencedUpdate,
    SequenceHeartbeat,
    Gap,
    Reconnect,
}

/// An explicit protocol observation. Snapshot/update rows must bind to an
/// actually applied normalized book by BOTH raw and receive clocks. apply_ns
/// is the evidence's causal replay position, not a fabricated venue timestamp.
/// For gap/reconnect observed only on the client, apply_ns == receive_ns.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookContinuityRecord {
    pub iid: String,
    pub token: String,
    pub event_epoch: u64,
    /// Per-token monotonically increasing recorder evidence identity, NOT a venue sequence.
    pub evidence_id: u64,
    pub kind: ContinuityKind,
    pub session_id: u64,
    pub sequence: Option<u64>,
    pub previous_sequence: Option<u64>,
    pub source_ns: Option<u64>,
    pub receive_ns: u64,
    pub apply_ns: u64,
    pub book_source_ns: Option<u64>,
    pub book_receive_ns: Option<u64>,
    pub provenance: String,
}

impl BookContinuityRecord {
    pub(crate) fn validate(&self, iid: &str) -> io::Result<()> {
        if self.iid != iid
            || self.token.is_empty()
            || self.provenance.is_empty()
            || self.session_id == 0
            || self.evidence_id == 0
            || self.receive_ns == 0
            || self.apply_ns == 0
            || self
                .source_ns
                .is_some_and(|source| source == 0 || source > self.apply_ns)
        {
            return Err(invalid(
                "invalid continuity identity, owner, timestamp or provenance",
            ));
        }
        match self.kind {
            ContinuityKind::Gap | ContinuityKind::Reconnect => {
                if self.source_ns.is_none() && self.apply_ns != self.receive_ns {
                    return Err(invalid(
                        "client-observed gap/reconnect must apply at recorded receive time",
                    ));
                }
            }
            _ => {
                let source = self.source_ns.ok_or_else(|| {
                    invalid("verified evidence requires a venue source timestamp")
                })?;
                if source == 0 || source > self.apply_ns || self.sequence.is_none() {
                    return Err(invalid(
                        "invalid continuity source/sequence or future watermark",
                    ));
                }
                if self.kind != ContinuityKind::SequenceHeartbeat
                    && (self.book_source_ns != Some(source)
                        || self.book_receive_ns != Some(self.receive_ns))
                {
                    return Err(invalid(
                        "snapshot/update evidence must identify its exact recorded book",
                    ));
                }
                if self.kind == ContinuityKind::SequencedUpdate && self.previous_sequence.is_none()
                {
                    return Err(invalid(
                        "sequenced update requires explicit previous sequence",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Copy-only token state. No allocations, logging, clock reads or implicit
/// heartbeat. A new normalized row can restore the HOLD ASSUMPTION after a
/// gap, but cannot restore verified sequence continuity by itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct BookContinuityState {
    pub status: ContinuityStatus,
    pub book_source_ns: Option<u64>,
    pub book_receive_ns: Option<u64>,
    pub book_identity_ambiguous: bool,
    pub session_id: Option<u64>,
    pub sequence: Option<u64>,
    pub verified_through_ns: Option<u64>,
    pub invalidated_receive_ns: Option<u64>,
    pub last_evidence_id: u64,
    pub last_evidence_source_ns: Option<u64>,
    pub duplicate_evidence: u64,
    pub ignored_older_evidence: u64,
    pub archive_certificate: Option<ArchiveCoverageCertificate>,
}

impl BookContinuityState {
    pub fn observe_book(&mut self, source_ns: u64, receive_ns: u64) {
        if self.book_source_ns.is_some_and(|last| source_ns < last) {
            return;
        }
        // Equal timestamps may be different book updates. No dedup inference.
        self.book_identity_ambiguous =
            self.book_source_ns == Some(source_ns) && self.book_receive_ns == Some(receive_ns);
        self.book_source_ns = Some(source_ns);
        self.book_receive_ns = Some(receive_ns);
        self.verified_through_ns = None;
        self.archive_certificate = None;
        if !self
            .invalidated_receive_ns
            .is_some_and(|invalidated| receive_ns <= invalidated)
        {
            self.status = ContinuityStatus::Unproven;
        }
    }

    pub fn observe(&mut self, record: &BookContinuityRecord) {
        if record.evidence_id == self.last_evidence_id {
            self.duplicate_evidence += 1;
            return;
        }
        if record.evidence_id < self.last_evidence_id {
            self.ignored_older_evidence += 1;
            return;
        }
        self.last_evidence_id = record.evidence_id;
        self.last_evidence_source_ns = record.source_ns;
        match record.kind {
            ContinuityKind::Gap | ContinuityKind::Reconnect => {
                // A stale connection's late disconnect cannot invalidate a
                // newer session already anchored by a full verified snapshot.
                if record.kind == ContinuityKind::Gap
                    && self.session_id.is_some_and(|s| s != record.session_id)
                {
                    self.ignored_older_evidence += 1;
                    return;
                }
                self.status = if record.kind == ContinuityKind::Gap {
                    ContinuityStatus::Gap
                } else {
                    ContinuityStatus::Reconnect
                };
                self.session_id = Some(record.session_id);
                self.sequence = None;
                self.verified_through_ns = None;
                self.invalidated_receive_ns = Some(
                    self.invalidated_receive_ns
                        .unwrap_or(0)
                        .max(record.receive_ns),
                );
            }
            ContinuityKind::Snapshot | ContinuityKind::SequencedUpdate => {
                // Delayed proof for an already superseded book must not turn a
                // newer observed book into a gap. Archive certificates retain
                // its historical coverage separately.
                if record
                    .book_source_ns
                    .zip(self.book_source_ns)
                    .is_some_and(|(proof, current)| proof < current)
                {
                    self.ignored_older_evidence += 1;
                    return;
                }
                let bound = record.book_source_ns == self.book_source_ns
                    && record.book_receive_ns == self.book_receive_ns
                    && self.book_source_ns.is_some()
                    && !self.book_identity_ambiguous
                    && !self
                        .invalidated_receive_ns
                        .is_some_and(|ns| record.receive_ns <= ns);
                let linked = record.kind == ContinuityKind::Snapshot
                    || (self.session_id == Some(record.session_id)
                        && self.sequence == record.previous_sequence
                        && self
                            .sequence
                            .zip(record.sequence)
                            .is_some_and(|(old, new)| new > old)
                        && !matches!(
                            self.status,
                            ContinuityStatus::Gap | ContinuityStatus::Reconnect
                        ));
                if !bound || !linked {
                    self.status = ContinuityStatus::Gap;
                    self.verified_through_ns = None;
                    self.invalidated_receive_ns = Some(
                        self.invalidated_receive_ns
                            .unwrap_or(0)
                            .max(record.receive_ns),
                    );
                    return;
                }
                self.status = ContinuityStatus::Verified;
                self.session_id = Some(record.session_id);
                self.sequence = record.sequence;
                self.verified_through_ns = record.source_ns;
            }
            ContinuityKind::SequenceHeartbeat => {
                if self.status == ContinuityStatus::Verified
                    && self.session_id == Some(record.session_id)
                    && self.sequence == record.sequence
                    && record
                        .source_ns
                        .zip(self.verified_through_ns)
                        .is_some_and(|(new, old)| new >= old)
                {
                    self.verified_through_ns = record.source_ns;
                } else {
                    self.status = ContinuityStatus::Gap;
                    self.verified_through_ns = None;
                    self.invalidated_receive_ns = Some(
                        self.invalidated_receive_ns
                            .unwrap_or(0)
                            .max(record.receive_ns),
                    );
                }
            }
        }
    }

    /// Causal watermark diagnostic only; admission uses an archive certificate.
    pub fn covered_at(&self, now_ns: u64) -> bool {
        self.status == ContinuityStatus::Verified
            && self.book_source_ns.is_some_and(|start| start <= now_ns)
            && self.verified_through_ns.is_some_and(|end| now_ns <= end)
    }

    pub fn hold_eligible(&self) -> bool {
        self.book_source_ns.is_some()
            && !matches!(
                self.status,
                ContinuityStatus::Gap | ContinuityStatus::Reconnect
            )
    }
}

/// Immutable metadata only. The closing message is verified at startup, but
/// its BBO is never read here or installed early. End is strictly exclusive;
/// terminal books and incomplete chains never receive an extrapolated end.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ArchiveCoverageCertificate {
    pub event_epoch: u64,
    pub session_id: u64,
    pub book_source_ns: u64,
    pub book_receive_ns: u64,
    pub start_ns: u64,
    pub end_exclusive_ns: u64,
    pub anchor_evidence_id: u64,
    pub closing_evidence_id: u64,
}
impl ArchiveCoverageCertificate {
    pub fn covers(&self, now_ns: u64) -> bool {
        self.start_ns <= now_ns && now_ns < self.end_exclusive_ns
    }
}

#[derive(Default)]
pub struct ArchiveCoverage {
    by_token: HashMap<String, Vec<ArchiveCoverageCertificate>>,
}
impl ArchiveCoverage {
    /// Startup-only verification of explicit protocol links. A snapshot starts
    /// a chain; only a linked delta or sequence-aware heartbeat closes coverage.
    fn from_records(records: &[BookContinuityRecord]) -> io::Result<Self> {
        #[derive(Clone, Copy)]
        struct Chain {
            cert: ArchiveCoverageCertificate,
            sequence: u64,
            last_source: u64,
        }
        let mut chains: HashMap<&str, Chain> = HashMap::new();
        let mut out = Self::default();
        let mut seen = std::collections::HashSet::new();
        let mut identities = std::collections::HashSet::new();
        for row in records {
            if !seen.insert((row.token.as_str(), row.evidence_id)) {
                continue;
            }
            if matches!(row.kind, ContinuityKind::Gap | ContinuityKind::Reconnect) {
                // A late gap from a retired connection cannot break the
                // independently anchored current session.
                if row.kind == ContinuityKind::Gap
                    && chains
                        .get(row.token.as_str())
                        .is_some_and(|chain| chain.cert.session_id != row.session_id)
                {
                    continue;
                }
                // Invalidations cannot create or extend a certificate.
                chains.remove(row.token.as_str());
                continue;
            }
            let source = row.source_ns.expect("validated source");
            if let Some(mut chain) = chains.remove(row.token.as_str()) {
                let linked = row.session_id == chain.cert.session_id
                    && row.event_epoch == chain.cert.event_epoch
                    && source >= chain.last_source
                    && match row.kind {
                        ContinuityKind::SequencedUpdate => {
                            row.previous_sequence == Some(chain.sequence)
                                && row.sequence.is_some_and(|seq| seq > chain.sequence)
                        }
                        ContinuityKind::SequenceHeartbeat => row.sequence == Some(chain.sequence),
                        _ => false,
                    };
                if linked {
                    chain.cert.end_exclusive_ns = source;
                    chain.cert.closing_evidence_id = row.evidence_id;
                    // Replace the last certificate for this anchor when a
                    // later heartbeat extends its already verified interval.
                    if source > chain.cert.start_ns {
                        let list = out.by_token.entry(row.token.clone()).or_default();
                        if list.last().is_some_and(|last| {
                            last.anchor_evidence_id == chain.cert.anchor_evidence_id
                        }) {
                            *list.last_mut().unwrap() = chain.cert;
                        } else {
                            list.push(chain.cert);
                        }
                    }
                    if row.kind == ContinuityKind::SequenceHeartbeat {
                        chain.last_source = source;
                        chains.insert(row.token.as_str(), chain);
                        continue;
                    }
                } else if row.kind != ContinuityKind::Snapshot {
                    continue; // broken chain: only a later full snapshot can re-anchor
                }
            } else if row.kind != ContinuityKind::Snapshot {
                continue;
            }
            if !identities.insert((row.token.as_str(), source, row.receive_ns)) {
                return Err(invalid("ambiguous archive book source/receive identity"));
            }
            chains.insert(
                row.token.as_str(),
                Chain {
                    cert: ArchiveCoverageCertificate {
                        event_epoch: row.event_epoch,
                        session_id: row.session_id,
                        book_source_ns: source,
                        book_receive_ns: row.receive_ns,
                        start_ns: source,
                        end_exclusive_ns: source,
                        anchor_evidence_id: row.evidence_id,
                        closing_evidence_id: row.evidence_id,
                    },
                    sequence: row.sequence.expect("validated sequence"),
                    last_source: source,
                },
            );
        }
        for list in out.by_token.values_mut() {
            list.sort_unstable_by_key(|c| (c.book_source_ns, c.book_receive_ns));
        }
        Ok(out)
    }
    pub fn certificate(
        &self,
        token: &str,
        source: u64,
        receive: u64,
        epoch: u64,
    ) -> Option<ArchiveCoverageCertificate> {
        let list = self.by_token.get(token)?;
        let index = list
            .binary_search_by_key(&(source, receive), |c| {
                (c.book_source_ns, c.book_receive_ns)
            })
            .ok()?;
        let cert = list[index];
        (cert.event_epoch == epoch).then_some(cert)
    }
    pub fn len(&self) -> usize {
        self.by_token.values().map(Vec::len).sum()
    }
}

pub struct BookContinuityReplay {
    pub(crate) owner_iid: String,
    pub(crate) records: Vec<BookContinuityRecord>,
    pub(crate) archive: ArchiveCoverage,
}

impl BookContinuityReplay {
    pub fn from_path(path: impl AsRef<Path>, expected_iid: &str) -> io::Result<Self> {
        Self::from_reader(BufReader::new(File::open(path)?), expected_iid)
    }
    pub fn from_reader(reader: impl BufRead, expected_iid: &str) -> io::Result<Self> {
        if expected_iid.is_empty() {
            return Err(invalid("continuity replay requires an explicit owner"));
        }
        let records: Vec<BookContinuityRecord> = read_jsonl(reader)?;
        let mut last_apply = 0;
        let mut identities = HashMap::new();
        let mut last_ids: HashMap<&str, u64> = HashMap::new();
        let mut last_sources: HashMap<&str, u64> = HashMap::new();
        for row in &records {
            row.validate(expected_iid)?;
            if row.apply_ns < last_apply {
                return Err(invalid("continuity journal is not sorted by apply_ns"));
            }
            last_apply = row.apply_ns;
            // Exact duplicate evidence is idempotent; conflicting reuse is a
            // malformed journal, rejected before the simulation starts.
            let key = (row.token.as_str(), row.evidence_id);
            let encoded = serde_json::to_vec(row).map_err(|e| invalid(&e.to_string()))?;
            if let Some(previous) = identities.insert(key, encoded.clone()) {
                if previous != encoded {
                    return Err(invalid("conflicting continuity evidence ID"));
                }
            } else {
                if last_ids
                    .get(row.token.as_str())
                    .is_some_and(|last| row.evidence_id <= *last)
                {
                    return Err(invalid(
                        "nonduplicate continuity evidence IDs must increase per token",
                    ));
                }
                last_ids.insert(row.token.as_str(), row.evidence_id);
                if let Some(source) = row.source_ns {
                    if last_sources
                        .get(row.token.as_str())
                        .is_some_and(|last| source < *last)
                    {
                        return Err(invalid(
                            "continuity source clock regressed; archive certification is unsafe",
                        ));
                    }
                    last_sources.insert(row.token.as_str(), source);
                }
            }
        }
        let archive = ArchiveCoverage::from_records(&records)?;
        Ok(Self {
            owner_iid: expected_iid.to_owned(),
            records,
            archive,
        })
    }
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn archive_certificate_count(&self) -> usize {
        self.archive.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrivalEvidenceKind {
    ModeledSplit,
    MeasuredClientInterval,
    MeasuredExchangePoint,
    Unknown,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceClockDomain {
    ClientWall,
    ExchangeWall,
    Unknown,
}

/// Bounds and selected matching point are separate. All existing simulator
/// splits remain modeled, even when client endpoints were measured. None clock
/// offsets explicitly mean that client bounds are not verified venue bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct ArrivalEvidence {
    pub kind: ArrivalEvidenceKind,
    pub lower_ns: Option<u64>,
    pub upper_ns: Option<u64>,
    pub selected_ns: u64,
    pub selected_kind: &'static str,
    pub clock_domain: EvidenceClockDomain,
    pub clock_offset_lower_ns: Option<i64>,
    pub clock_offset_upper_ns: Option<i64>,
    pub provenance_row: Option<u64>,
}

impl ArrivalEvidence {
    pub fn modeled(selected_ns: u64) -> Self {
        Self {
            kind: ArrivalEvidenceKind::ModeledSplit,
            lower_ns: None,
            upper_ns: None,
            selected_ns,
            selected_kind: "modeled_split",
            clock_domain: EvidenceClockDomain::Unknown,
            clock_offset_lower_ns: None,
            clock_offset_upper_ns: None,
            provenance_row: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArrivalEvidenceRecord {
    pub iid: String,
    pub coid: String,
    pub token: String,
    pub event_epoch: u64,
    pub epoch: u64,
    pub event_id: String,
    pub command_kind: String,
    pub attempt_id: u64,
    pub dispatched_ns: u64,
    pub completed_ns: Option<u64>,
    pub kind: ArrivalEvidenceKind,
    pub lower_ns: Option<u64>,
    pub upper_ns: Option<u64>,
    pub clock_domain: EvidenceClockDomain,
    pub clock_offset_lower_ns: Option<i64>,
    pub clock_offset_upper_ns: Option<i64>,
    pub provenance: String,
}

impl ArrivalEvidenceRecord {
    fn validate(&self) -> io::Result<()> {
        if self.iid.is_empty()
            || self.coid.is_empty()
            || self.token.is_empty()
            || self.provenance.is_empty()
            || self.epoch != self.event_epoch
            || self.event_id.is_empty()
            || self.command_kind != "place"
            || self.dispatched_ns == 0
            || self.completed_ns.is_some_and(|t| t < self.dispatched_ns)
            || self.lower_ns.zip(self.upper_ns).is_some_and(|(a, b)| b < a)
            || self.clock_offset_lower_ns.is_some() != self.clock_offset_upper_ns.is_some()
            || self
                .clock_offset_lower_ns
                .zip(self.clock_offset_upper_ns)
                .is_some_and(|(a, b)| b < a)
        {
            return Err(invalid(
                "invalid arrival identity, interval, or clock-offset bounds",
            ));
        }
        match self.kind {
            ArrivalEvidenceKind::MeasuredClientInterval => {
                if self.clock_domain != EvidenceClockDomain::ClientWall
                    || self.lower_ns != Some(self.dispatched_ns)
                    || self
                        .upper_ns
                        .is_some_and(|upper| self.completed_ns != Some(upper))
                {
                    return Err(invalid(
                        "client arrival interval must use dispatched/completed endpoints",
                    ));
                }
            }
            ArrivalEvidenceKind::MeasuredExchangePoint => {
                if self.clock_domain != EvidenceClockDomain::ExchangeWall
                    || self.lower_ns.is_none_or(|point| point == 0)
                    || self.lower_ns != self.upper_ns
                {
                    return Err(invalid(
                        "measured exchange point requires an explicit exchange-domain point",
                    ));
                }
            }
            ArrivalEvidenceKind::Unknown => {
                if self.upper_ns.is_some() {
                    return Err(invalid("unknown arrival cannot assert an upper bound"));
                }
            }
            ArrivalEvidenceKind::ModeledSplit => {
                return Err(invalid(
                    "external arrival evidence cannot masquerade modeled splits as observations",
                ))
            }
        }
        Ok(())
    }
    pub fn for_selected(&self, selected_ns: u64, row: u64) -> ArrivalEvidence {
        ArrivalEvidence {
            kind: self.kind,
            lower_ns: self.lower_ns,
            upper_ns: self.upper_ns,
            selected_ns,
            selected_kind: "modeled_split",
            clock_domain: self.clock_domain,
            clock_offset_lower_ns: self.clock_offset_lower_ns,
            clock_offset_upper_ns: self.clock_offset_upper_ns,
            provenance_row: Some(row),
        }
    }
}

/// Startup-only exact coid lookup for fixed recorded PLACE commands. Full
/// strategy simulations never install this table or reuse historical IDs.
pub struct ArrivalEvidenceReplay {
    rows: HashMap<String, (u64, ArrivalEvidenceRecord)>,
}
impl ArrivalEvidenceReplay {
    pub fn from_path(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::from_reader(BufReader::new(File::open(path)?))
    }
    pub fn from_reader(reader: impl BufRead) -> io::Result<Self> {
        let records: Vec<ArrivalEvidenceRecord> = read_jsonl(reader)?;
        let mut rows = HashMap::with_capacity(records.len());
        for (index, record) in records.into_iter().enumerate() {
            record.validate()?;
            if rows
                .insert(record.coid.clone(), (index as u64 + 1, record))
                .is_some()
            {
                return Err(invalid("duplicate arrival coid"));
            }
        }
        Ok(Self { rows })
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn get(&self, coid: &str) -> Option<&(u64, ArrivalEvidenceRecord)> {
        self.rows.get(coid)
    }
}

fn read_jsonl<T: serde::de::DeserializeOwned>(mut reader: impl BufRead) -> io::Result<Vec<T>> {
    let mut rows = Vec::new();
    let mut bytes = Vec::with_capacity(MAX_LINE_BYTES + 1);
    loop {
        bytes.clear();
        let count = std::io::Read::take(&mut reader, MAX_LINE_BYTES as u64 + 1)
            .read_until(b'\n', &mut bytes)?;
        if count == 0 {
            break;
        }
        if count > MAX_LINE_BYTES {
            return Err(invalid("evidence JSONL line capacity exceeded"));
        }
        if rows.len() == MAX_EVIDENCE_ROWS {
            return Err(invalid("evidence JSONL row capacity exceeded"));
        }
        rows.push(
            serde_json::from_slice(&bytes)
                .map_err(|e| invalid(&format!("evidence JSONL row {}: {e}", rows.len() + 1)))?,
        );
    }
    Ok(rows)
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn record(id: u64, kind: ContinuityKind, source: u64, receive: u64) -> BookContinuityRecord {
        BookContinuityRecord {
            iid: "owner".into(),
            token: "up".into(),
            event_epoch: 300,
            evidence_id: id,
            kind,
            session_id: 1,
            sequence: Some(id),
            previous_sequence: id.checked_sub(1),
            source_ns: Some(source),
            receive_ns: receive,
            apply_ns: source,
            book_source_ns: Some(source),
            book_receive_ns: Some(receive),
            provenance: "fixture:raw_protocol".into(),
        }
    }
    fn jsonl<T: Serialize>(rows: &[T]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for row in rows {
            serde_json::to_writer(&mut bytes, row).unwrap();
            bytes.push(b'\n');
        }
        bytes
    }

    #[test]
    fn normalized_snapshot_and_time_never_claim_continuity() {
        let mut state = BookContinuityState::default();
        state.observe_book(100, 120);
        assert!(state.hold_eligible());
        for now in [100, 101, 2_000_000_100, u64::MAX] {
            assert!(!state.covered_at(now));
        }
        assert_eq!(state.verified_through_ns, None);
    }

    #[test]
    fn sequence_links_and_protocol_watermarks_are_causal() {
        let mut state = BookContinuityState::default();
        state.observe_book(100, 120);
        state.observe(&record(1, ContinuityKind::Snapshot, 100, 120));
        assert!(state.covered_at(100));
        assert!(!state.covered_at(101));
        let mut heartbeat = record(2, ContinuityKind::SequenceHeartbeat, 140, 160);
        heartbeat.sequence = Some(1);
        state.observe(&heartbeat);
        assert!(state.covered_at(140));
        assert!(!state.covered_at(141));
        state.observe_book(150, 170);
        let mut update = record(3, ContinuityKind::SequencedUpdate, 150, 170);
        update.sequence = Some(2);
        update.previous_sequence = Some(1);
        state.observe(&update);
        assert!(state.covered_at(150));
        assert_eq!(state.sequence, Some(2));
        let mut skipped = record(4, ContinuityKind::SequencedUpdate, 160, 180);
        skipped.previous_sequence = Some(3);
        state.observe_book(160, 180);
        state.observe(&skipped);
        assert_eq!(state.status, ContinuityStatus::Gap);
        assert!(!state.hold_eligible());
    }

    #[test]
    fn duplicate_and_older_protocol_evidence_never_advance_watermark() {
        let mut state = BookContinuityState::default();
        state.observe_book(100, 120);
        let snapshot = record(5, ContinuityKind::Snapshot, 100, 120);
        state.observe(&snapshot);
        state.observe(&snapshot);
        state.observe(&record(4, ContinuityKind::Snapshot, 99, 119));
        assert_eq!(state.verified_through_ns, Some(100));
        assert_eq!(
            (state.duplicate_evidence, state.ignored_older_evidence),
            (1, 1)
        );
    }

    #[test]
    fn equal_raw_books_are_legal_but_ambiguous_identity_is_not_verified() {
        let mut state = BookContinuityState::default();
        state.observe_book(100, 120);
        state.observe_book(100, 121);
        assert!(!state.book_identity_ambiguous);
        state.observe(&record(1, ContinuityKind::Snapshot, 100, 121));
        assert!(state.covered_at(100));
        state.observe_book(100, 121);
        assert!(state.book_identity_ambiguous);
        state.observe(&record(2, ContinuityKind::Snapshot, 100, 121));
        assert!(!state.covered_at(100));
    }

    #[test]
    fn gap_reconnect_require_new_observed_book_and_new_snapshot_anchor() {
        let mut state = BookContinuityState::default();
        state.observe_book(100, 120);
        state.observe(&record(1, ContinuityKind::Snapshot, 100, 120));
        let mut reconnect = record(2, ContinuityKind::Reconnect, 130, 150);
        reconnect.session_id = 2;
        state.observe(&reconnect);
        assert!(!state.hold_eligible());
        state.observe_book(125, 145);
        assert!(
            !state.hold_eligible(),
            "old-session delayed book cannot heal reconnect"
        );
        state.observe_book(160, 180);
        assert!(state.hold_eligible());
        assert!(!state.covered_at(160));
        let mut next = record(3, ContinuityKind::Snapshot, 160, 180);
        next.session_id = 2;
        state.observe(&next);
        assert!(state.covered_at(160));
        let mut old_gap = record(4, ContinuityKind::Gap, 165, 185);
        old_gap.session_id = 1;
        state.observe(&old_gap);
        assert_eq!(state.status, ContinuityStatus::Verified);
    }

    #[test]
    fn owner_token_states_and_sessions_are_isolated() {
        let mut a = BookContinuityState::default();
        let b = BookContinuityState::default();
        a.observe_book(100, 120);
        a.observe(&record(1, ContinuityKind::Snapshot, 100, 120));
        assert!(a.covered_at(100));
        assert!(!b.hold_eligible());
        let mut wrong_session = record(2, ContinuityKind::SequenceHeartbeat, 110, 130);
        wrong_session.session_id = 2;
        a.observe(&wrong_session);
        assert!(!a.hold_eligible());
        assert!(BookContinuityReplay::from_reader(
            Cursor::new(jsonl(&[record(1, ContinuityKind::Snapshot, 100, 120)])),
            "foreign"
        )
        .is_err());
    }

    #[test]
    fn journal_rejects_future_watermarks_conflicting_ids_and_overflow() {
        let good = record(1, ContinuityKind::Snapshot, 100, 120);
        let mut future = good.clone();
        future.apply_ns = 99;
        assert!(BookContinuityReplay::from_reader(Cursor::new(jsonl(&[future])), "owner").is_err());
        let mut conflicting = good.clone();
        conflicting.sequence = Some(9);
        assert!(BookContinuityReplay::from_reader(
            Cursor::new(jsonl(&[good.clone(), conflicting])),
            "owner"
        )
        .is_err());
        assert_eq!(
            BookContinuityReplay::from_reader(Cursor::new(jsonl(&[good.clone(), good])), "owner")
                .unwrap()
                .len(),
            2
        );
        assert!(
            read_jsonl::<serde_json::Value>(Cursor::new(vec![b' '; MAX_LINE_BYTES + 1])).is_err()
        );
        let rows = b"{}\n".repeat(MAX_EVIDENCE_ROWS + 1);
        assert!(read_jsonl::<serde_json::Value>(Cursor::new(rows)).is_err());
    }

    #[test]
    fn archive_intervals_cover_between_updates_without_future_prices() {
        let first = record(1, ContinuityKind::Snapshot, 100, 120);
        let mut heartbeat = record(2, ContinuityKind::SequenceHeartbeat, 150, 170);
        heartbeat.sequence = Some(1);
        let mut update = record(3, ContinuityKind::SequencedUpdate, 200, 220);
        update.sequence = Some(2);
        update.previous_sequence = Some(1);
        let replay = BookContinuityReplay::from_reader(
            Cursor::new(jsonl(&[first.clone(), heartbeat, update])),
            "owner",
        )
        .unwrap();
        let cert = replay.archive.certificate("up", 100, 120, 300).unwrap();
        assert_eq!(
            (
                cert.start_ns,
                cert.end_exclusive_ns,
                cert.closing_evidence_id
            ),
            (100, 200, 3)
        );
        assert!(!cert.covers(99));
        assert!(cert.covers(100));
        assert!(cert.covers(199));
        assert!(!cert.covers(200));
        assert!(
            replay.archive.certificate("up", 200, 220, 300).is_none(),
            "terminal book has no future interval"
        );
        assert!(replay.archive.certificate("down", 100, 120, 300).is_none());
        assert!(replay.archive.certificate("up", 100, 121, 300).is_none());
        assert!(replay.archive.certificate("up", 100, 120, 600).is_none());
        let mut delayed = first;
        delayed.apply_ns = 125;
        let mut next = record(2, ContinuityKind::SequencedUpdate, 200, 220);
        next.apply_ns = 225;
        let archive =
            BookContinuityReplay::from_reader(Cursor::new(jsonl(&[delayed, next])), "owner")
                .unwrap()
                .archive;
        assert!(
            archive
                .certificate("up", 100, 120, 300)
                .unwrap()
                .covers(150),
            "coverage is retrospective metadata, not causal-watermark equality"
        );
    }

    #[test]
    fn archive_gap_reconnect_and_unlinked_delta_never_bridge_missing_data() {
        let first = record(1, ContinuityKind::Snapshot, 100, 120);
        let mut gap = record(2, ContinuityKind::Gap, 150, 170);
        gap.source_ns = None;
        gap.apply_ns = 170;
        let update = record(3, ContinuityKind::SequencedUpdate, 200, 220);
        let replay = BookContinuityReplay::from_reader(
            Cursor::new(jsonl(&[first.clone(), gap.clone(), update.clone()])),
            "owner",
        )
        .unwrap();
        assert_eq!(replay.archive.len(), 0);
        let mut broken = update;
        broken.previous_sequence = Some(999);
        assert_eq!(
            BookContinuityReplay::from_reader(Cursor::new(jsonl(&[first, broken])), "owner")
                .unwrap()
                .archive
                .len(),
            0
        );
        let mut second = record(1, ContinuityKind::Snapshot, 100, 120);
        second.session_id = 2;
        gap.session_id = 1;
        let mut next = record(3, ContinuityKind::SequencedUpdate, 200, 220);
        next.session_id = 2;
        next.sequence = Some(2);
        next.previous_sequence = Some(1);
        let replay = BookContinuityReplay::from_reader(
            Cursor::new(jsonl(&[second.clone(), gap.clone(), next.clone()])),
            "owner",
        )
        .unwrap();
        assert!(
            replay
                .archive
                .certificate("up", 100, 120, 300)
                .unwrap()
                .covers(175),
            "late old-session gap cannot invalidate current session"
        );
        gap.kind = ContinuityKind::Reconnect;
        assert_eq!(
            BookContinuityReplay::from_reader(Cursor::new(jsonl(&[second, gap, next])), "owner")
                .unwrap()
                .archive
                .len(),
            0
        );
    }

    #[test]
    fn journal_rejects_decreasing_identity_and_future_gap_before_mutation() {
        let first = record(100, ContinuityKind::Snapshot, 100, 120);
        let gap = record(50, ContinuityKind::Gap, 200, 220);
        assert!(
            BookContinuityReplay::from_reader(Cursor::new(jsonl(&[first, gap])), "owner").is_err()
        );
        for kind in [ContinuityKind::Gap, ContinuityKind::Reconnect] {
            let mut future = record(1, kind, 200, 220);
            future.apply_ns = 100;
            assert!(
                BookContinuityReplay::from_reader(Cursor::new(jsonl(&[future])), "owner").is_err()
            );
        }
    }

    fn arrival() -> ArrivalEvidenceRecord {
        ArrivalEvidenceRecord {
            iid: "owner".into(),
            coid: "opaque-id".into(),
            token: "up".into(),
            event_epoch: 300,
            epoch: 300,
            event_id: "actual-event".into(),
            command_kind: "place".into(),
            attempt_id: 3,
            dispatched_ns: 100,
            completed_ns: Some(200),
            kind: ArrivalEvidenceKind::MeasuredClientInterval,
            lower_ns: Some(100),
            upper_ns: Some(200),
            clock_domain: EvidenceClockDomain::ClientWall,
            clock_offset_lower_ns: None,
            clock_offset_upper_ns: None,
            provenance: "fixture:dispatch/completed".into(),
        }
    }
    #[test]
    fn measured_client_bounds_do_not_become_a_measured_arrival_point() {
        let row = arrival();
        row.validate().unwrap();
        let evidence = row.for_selected(150, 1);
        assert_eq!(evidence.selected_kind, "modeled_split");
        assert_eq!(evidence.selected_ns, 150);
        assert_eq!(evidence.clock_offset_lower_ns, None);
        let mut timeout = row.clone();
        timeout.upper_ns = None;
        timeout.validate().unwrap();
        assert_eq!(timeout.for_selected(150, 1).upper_ns, None);
        assert!(
            ArrivalEvidenceReplay::from_reader(Cursor::new(jsonl(&[row.clone(), row]))).is_err()
        );
    }
    #[test]
    fn invalid_arrival_domain_or_identity_fails_before_replay() {
        let mut row = arrival();
        row.clock_domain = EvidenceClockDomain::ExchangeWall;
        assert!(row.validate().is_err());
        let mut row = arrival();
        row.epoch = 600;
        assert!(row.validate().is_err());
        let mut row = arrival();
        row.upper_ns = Some(201);
        assert!(row.validate().is_err());
        let mut row = arrival();
        row.upper_ns = Some(101);
        assert!(row.validate().is_err());
        let mut row = arrival();
        row.kind = ArrivalEvidenceKind::MeasuredExchangePoint;
        row.clock_domain = EvidenceClockDomain::ExchangeWall;
        row.lower_ns = Some(0);
        row.upper_ns = Some(0);
        assert!(row.validate().is_err());
        let mut row = arrival();
        row.kind = ArrivalEvidenceKind::MeasuredExchangePoint;
        assert!(row.validate().is_err());
    }

    /// Lookup only: immutable archive identity -> copied interval -> covers.
    /// Startup chain parsing and timing storage are outside the measurement.
    #[test]
    #[ignore = "focused offline benchmark; release --ignored --nocapture"]
    fn archive_lookup_benchmark() {
        const N: usize = 100_000;
        const CERTS: usize = 4096;
        let list = (0..CERTS)
            .map(|i| ArchiveCoverageCertificate {
                event_epoch: 300,
                session_id: 1,
                book_source_ns: i as u64 * 100 + 1,
                book_receive_ns: i as u64 * 100 + 2,
                start_ns: i as u64 * 100 + 1,
                end_exclusive_ns: i as u64 * 100 + 101,
                anchor_evidence_id: i as u64 + 1,
                closing_evidence_id: i as u64 + 2,
            })
            .collect();
        let archive = ArchiveCoverage {
            by_token: HashMap::from([("up".into(), list)]),
        };
        let mut timings = Vec::with_capacity(N);
        for i in 0..N {
            let source = (i % CERTS) as u64 * 100 + 1;
            let before = std::time::Instant::now();
            std::hint::black_box(
                archive
                    .certificate("up", source, source + 1, 300)
                    .unwrap()
                    .covers(source + 50),
            );
            timings.push(before.elapsed().as_nanos());
        }
        timings.sort_unstable();
        println!("archive_lookup_benchmark scope=immutable_identity_lookup_and_interval_check events={N} certificates={CERTS} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_capacity=0 queue_high_water=0 overflow=0 archive_hard_capacity={}",
            timings[N/2], timings[N*99/100], timings[N*999/1000], timings[N-1], MAX_EVIDENCE_ROWS);
    }

    /// Boundary: mutable continuity observation + eligibility query only.
    /// Input strings and timings storage are allocated before measurement.
    #[test]
    #[ignore = "focused offline benchmark; release --ignored --nocapture"]
    fn continuity_state_benchmark() {
        const N: usize = 100_000;
        for mode in [
            BookContinuityMode::LegacyAge,
            BookContinuityMode::SnapshotHoldEstimated,
            BookContinuityMode::RequireVerified,
        ] {
            let mut state = BookContinuityState::default();
            let mut proof = record(1, ContinuityKind::Snapshot, 1, 2);
            let mut timings = Vec::with_capacity(N);
            for n in 0..N {
                let source = n as u64 + 1;
                proof.evidence_id = source;
                proof.source_ns = Some(source);
                proof.receive_ns = source + 1;
                proof.book_source_ns = Some(source);
                proof.book_receive_ns = Some(source + 1);
                let before = std::time::Instant::now();
                match mode {
                    BookContinuityMode::LegacyAge => {
                        std::hint::black_box(source.saturating_sub(source) <= 2_000_000_000);
                    }
                    BookContinuityMode::SnapshotHoldEstimated => {
                        state.observe_book(source, source + 1);
                        std::hint::black_box(state.hold_eligible());
                    }
                    BookContinuityMode::RequireVerified => {
                        state.observe_book(source, source + 1);
                        state.observe(&proof);
                        std::hint::black_box(state.covered_at(source));
                    }
                }
                timings.push(before.elapsed().as_nanos());
            }
            timings.sort_unstable();
            println!("continuity_state_benchmark mode={mode:?} events={N} median_ns={} p99_ns={} p999_ns={} max_ns={} state_capacity=1 queue_capacity=0 queue_high_water=0 overflow=0", timings[N/2], timings[N*99/100], timings[N*999/1000], timings[N-1]);
        }
    }
}
