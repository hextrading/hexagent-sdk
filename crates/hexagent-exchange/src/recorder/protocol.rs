//! Collector DTO. This is a recorder-worker API, not an assertion that
//! old normalized tapes contain raw protocol evidence. The exchange parser must
//! fill these fields before normalization, and a bounded owner-routed message
//! must carry them to the existing recorder worker. Current live SDK pins do
//! not call this new API; integrating that lane is a separate live change.

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookProtocolKind {
    Snapshot,
    Delta,
    SequenceHeartbeat,
    Gap,
    Reconnect,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookProtocolOwnerScope {
    #[default]
    StrategyInstance,
    /// Public WS authority only. Binding to a strategy needs an independently
    /// frozen subscription/owner mapping; this is never an instance journal.
    PublicFeed,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookProtocolRecord {
    #[serde(default)]
    pub owner_scope: BookProtocolOwnerScope,
    /// Strategy iid in the legacy scope; explicit feed identity in PublicFeed.
    pub iid: String,
    pub token: String,
    pub event_epoch: u64,
    pub connection_id: u64,
    pub session_id: u64,
    pub kind: BookProtocolKind,
    pub wire_message_type: String,
    /// Only populated from an actual venue protocol sequence field.
    pub venue_sequence: Option<u64>,
    pub venue_previous_sequence: Option<u64>,
    /// Local observation identity only; NEVER a venue continuity proof.
    pub recorder_sequence: u64,
    pub exchange_timestamp_ns: Option<u64>,
    pub local_timestamp_ns: u64,
    pub venue_book_hash: Option<String>,
    #[serde(default)]
    pub event_id: Option<String>,
    /// Original numeric timestamp as received, before ms->ns conversion.
    #[serde(default)]
    pub raw_exchange_timestamp: Option<u64>,
    /// Local raw-frame identity, never a venue sequence.
    #[serde(default)]
    pub frame_sequence: u64,
}

impl BookProtocolRecord {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.iid.is_empty()
            || self.iid.len() > 128
            || self.token.is_empty()
            || self.token.len() > 128
            || self.wire_message_type.is_empty()
            || self.wire_message_type.len() > 128
            || self.connection_id == 0
            || self.session_id == 0
            || self.recorder_sequence == 0
            || self.local_timestamp_ns == 0
            || self
                .venue_book_hash
                .as_ref()
                .is_some_and(|hash| hash.len() > 256)
            || self.exchange_timestamp_ns == Some(0)
        {
            return Err("invalid bounded protocol record identity or timestamp");
        }
        if matches!(self.kind, BookProtocolKind::SequenceHeartbeat) && self.venue_sequence.is_none()
        {
            return Err(
                "a generic heartbeat without a venue sequence cannot certify book continuity",
            );
        }
        Ok(())
    }

    pub fn validate_strategy_owner(&self, iid: &str) -> Result<(), &'static str> {
        self.validate()?;
        if self.owner_scope != BookProtocolOwnerScope::StrategyInstance || self.iid != iid {
            return Err("public-feed evidence requires explicit offline instance binding");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn protocol_record_keeps_venue_and_recorder_identity_separate() {
        let mut row = BookProtocolRecord {
            owner_scope: BookProtocolOwnerScope::StrategyInstance,
            iid: "owner".into(),
            token: "up".into(),
            event_epoch: 300,
            connection_id: 1,
            session_id: 2,
            kind: BookProtocolKind::Snapshot,
            wire_message_type: "book".into(),
            venue_sequence: None,
            venue_previous_sequence: None,
            recorder_sequence: 123,
            exchange_timestamp_ns: None,
            local_timestamp_ns: 500,
            venue_book_hash: None,
            event_id: None,
            raw_exchange_timestamp: None,
            frame_sequence: 0,
        };
        row.validate().unwrap();
        let decoded: BookProtocolRecord =
            serde_json::from_slice(&serde_json::to_vec(&row).unwrap()).unwrap();
        assert_eq!(decoded.venue_sequence, None);
        assert_eq!(decoded.recorder_sequence, 123);
        assert_eq!(decoded.exchange_timestamp_ns, None);
        row.kind = BookProtocolKind::SequenceHeartbeat;
        assert!(row.validate().is_err());
        row.venue_sequence = Some(9);
        row.validate().unwrap();
        row.wire_message_type = "x".repeat(129);
        assert!(row.validate().is_err());
    }
}
