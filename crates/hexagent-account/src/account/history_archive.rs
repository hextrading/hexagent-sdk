//! Disk-only historical replay proofs. Connections and serialization are used
//! only by startup or the existing cold account owner. Private/strategy owners
//! read a fixed-size published membership filter and send an explicit message.
use super::*;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use std::sync::atomic::AtomicU8;

pub(super) const HOT_RETENTION_MS: u64 = 30 * 60 * 1_000;
const FILTER_BYTES: usize = 512 * 1024;
const MAX_LOOKUP_ROWS: usize = 128;
const ABSENCE_SLOTS: usize = 64;
const ABSENCE_KEY_BYTES: usize = 128;

// Single cold writer, bounded exact-key negative certificates. Atomic bytes
// avoid allocation, locks, and last-reader destruction on a private owner.
// SeqCst sequence + payload reads reject any overlapping slot replacement.
#[derive(Debug)]
struct VerifiedAbsence {
    sequence: AtomicU64,
    generation: AtomicU64,
    fingerprint: AtomicU64,
    length: AtomicUsize,
    key: [AtomicU8; ABSENCE_KEY_BYTES],
}
impl VerifiedAbsence {
    fn new() -> Self {
        Self {
            sequence: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            fingerprint: AtomicU64::new(0),
            length: AtomicUsize::new(0),
            key: std::array::from_fn(|_| AtomicU8::new(0)),
        }
    }
}
fn absence_fingerprint(key: &str) -> u64 {
    key.bytes().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x100000001b3)
    })
}

#[derive(Debug)]
pub(super) struct HistoryArchive {
    path: PathBuf,
    account_id: String,
    // Cold owner publishes append-only bits; readers never own/drop an old
    // heap buffer on the private lane. This is advisory, not ledger authority.
    filter: Box<[AtomicU8]>,
    generation: AtomicU64,
    absence_cursor: AtomicUsize, // only startup / the cold account owner writes
    absences: Box<[VerifiedAbsence]>,
}

#[derive(Default)]
pub(super) struct ArchiveRows {
    pub trades: Vec<(String, RetiredTradeOwnershipTombstone)>,
    pub orders: Vec<(String, RetiredOrderAuditTombstone)>,
}
impl ArchiveRows {
    pub fn len(&self) -> usize {
        self.trades.len() + self.orders.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// Stable across Rust/compiler versions. False positives only cause a cold
// lookup; this filter is NEVER authority to suppress or apply an event.
fn filter_indices(kind: u8, key: &str) -> [usize; 4] {
    let mut hash = 0xcbf29ce484222325u64 ^ u64::from(kind);
    let key = if kind == 2 {
        key.trim()
            .strip_prefix("0x")
            .or_else(|| key.trim().strip_prefix("0X"))
            .unwrap_or(key.trim())
    } else {
        key
    };
    for byte in key.bytes() {
        let byte = if kind == 2 {
            byte.to_ascii_lowercase()
        } else {
            byte
        };
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3);
    }
    let step = hash.rotate_left(31) | 1;
    std::array::from_fn(|i| {
        hash.wrapping_add((i as u64).wrapping_mul(step)) as usize % (FILTER_BYTES * 8)
    })
}
fn add_filter(filter: &mut [u8], kind: u8, key: &str) {
    for bit in filter_indices(kind, key) {
        filter[bit / 8] |= 1 << (bit % 8);
    }
}
fn base_trade_key(key: &str) -> &str {
    key.split_once(':').map_or(key, |(base, _)| base)
}

impl HistoryArchive {
    pub fn open(ledger: &Path, account_id: &str, required_generation: u64) -> Result<Self, String> {
        let mut name = ledger.as_os_str().to_os_string();
        name.push(".history.sqlite3");
        let path = PathBuf::from(name);
        if required_generation != 0 && !path.is_file() {
            return Err(format!(
                "required account history archive missing: {} generation={required_generation}",
                path.display()
            ));
        }
        let new_file = !path.exists();
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
        let connection = Connection::open_with_flags(&path, flags).map_err(|e| e.to_string())?;
        configure(&connection)?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL, account_id TEXT NOT NULL, generation INTEGER NOT NULL, filter BLOB NOT NULL, filter_checksum TEXT NOT NULL); CREATE TABLE IF NOT EXISTS proof (kind INTEGER NOT NULL, key TEXT NOT NULL, base_key TEXT NOT NULL, payload BLOB NOT NULL, checksum TEXT NOT NULL, PRIMARY KEY(kind,key)) WITHOUT ROWID; CREATE INDEX IF NOT EXISTS proof_base ON proof(kind,base_key);").map_err(|e| e.to_string())?;
        connection
            .execute(
                "INSERT OR IGNORE INTO metadata VALUES (1,1,?1,1,?2,?3)",
                params![
                    account_id,
                    vec![0u8; FILTER_BYTES],
                    format!("{:016x}", persistence_checksum(&vec![0u8; FILTER_BYTES]))
                ],
            )
            .map_err(|e| e.to_string())?;
        let (version, stored_account, generation, filter, filter_checksum): (u32, String, u64, Vec<u8>, String) = connection
            .query_row(
                "SELECT version,account_id,generation,filter,filter_checksum FROM metadata WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .map_err(|e| e.to_string())?;
        if version != 1
            || stored_account != account_id
            || generation < required_generation
            || filter.len() != FILTER_BYTES
            || filter_checksum != format!("{:016x}", persistence_checksum(&filter))
        {
            return Err("account history archive identity/generation/filter mismatch".into());
        }
        let check: String = connection
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .map_err(|e| e.to_string())?;
        if check != "ok" {
            return Err(format!("account history archive integrity: {check}"));
        }
        if new_file {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            std::fs::File::open(parent)
                .and_then(|file| file.sync_all())
                .map_err(|error| format!("sync archive directory: {error}"))?;
        }
        Ok(Self {
            path,
            account_id: account_id.to_owned(),
            filter: filter.into_iter().map(AtomicU8::new).collect(),
            generation: AtomicU64::new(generation),
            absence_cursor: AtomicUsize::new(0),
            absences: (0..ABSENCE_SLOTS).map(|_| VerifiedAbsence::new()).collect(),
        })
    }

    pub(super) fn connection(&self, write: bool) -> Result<Connection, String> {
        // Never CREATE here: disappearance after successful open must fail closed.
        let flags = if write {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        } else {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        };
        let connection = Connection::open_with_flags(&self.path, flags)
            .map_err(|e| format!("open history archive: {e}"))?;
        configure(&connection)?;
        let identity: (u32, String) = connection
            .query_row(
                "SELECT version,account_id FROM metadata WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|e| e.to_string())?;
        if identity != (1, self.account_id.clone()) {
            return Err("account history archive identity changed".into());
        }
        Ok(connection)
    }

    pub fn may_contain(&self, kind: u8, key: &str) -> bool {
        if key.is_empty() {
            return false;
        }
        filter_indices(kind, key)
            .iter()
            .all(|bit| self.filter[bit / 8].load(Ordering::Acquire) & (1 << (bit % 8)) != 0)
    }

    /// Advisory positives must be resolved before ANY SDK caller can create a
    /// new economic row. Only exact cold-verified absence at this generation
    /// permits a Bloom false positive to proceed.
    pub(super) fn unverified_trade_hint(&self, trade_key: &str) -> bool {
        let key = base_trade_key(trade_key);
        if !self.may_contain(1, key) {
            return false;
        }
        let generation = self.generation.load(Ordering::SeqCst);
        let fingerprint = absence_fingerprint(key);
        for slot in &self.absences {
            let sequence = slot.sequence.load(Ordering::SeqCst);
            if sequence == 0
                || sequence % 2 != 0
                || slot.generation.load(Ordering::SeqCst) != generation
                || slot.fingerprint.load(Ordering::SeqCst) != fingerprint
                || slot.length.load(Ordering::SeqCst) != key.len()
                || key.len() > ABSENCE_KEY_BYTES
            {
                continue;
            }
            let exact = key
                .bytes()
                .zip(&slot.key)
                .all(|(byte, stored)| stored.load(Ordering::SeqCst) == byte);
            if exact
                && slot.sequence.load(Ordering::SeqCst) == sequence
                && self.generation.load(Ordering::SeqCst) == generation
            {
                return false;
            }
        }
        true
    }

    // Called only after successful exact lookup by the sole cold writer. The
    // generation changes before newly archived proofs can leave hot memory.
    fn record_verified_absence(&self, key: &str) -> Result<(), String> {
        if key.len() > ABSENCE_KEY_BYTES {
            return Err("archive absence identity exceeds bounded certificate".into());
        }
        let index = self.absence_cursor.fetch_add(1, Ordering::Relaxed) % ABSENCE_SLOTS;
        let slot = &self.absences[index];
        slot.sequence.fetch_add(1, Ordering::SeqCst);
        slot.generation
            .store(self.generation.load(Ordering::SeqCst), Ordering::SeqCst);
        slot.fingerprint
            .store(absence_fingerprint(key), Ordering::SeqCst);
        slot.length.store(key.len(), Ordering::SeqCst);
        for (stored, byte) in slot.key.iter().zip(key.bytes()) {
            stored.store(byte, Ordering::SeqCst);
        }
        slot.sequence.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Commit proofs and membership in one FULL-synchronous transaction BEFORE
    /// their removal is allowed into the ledger WAL. A crash between commits
    /// leaves redundant proofs; it cannot leave a ledger without its archive.
    pub fn store(&self, rows: &ArchiveRows) -> Result<u64, String> {
        let mut connection = self.connection(true)?;
        let tx = connection.transaction().map_err(|e| e.to_string())?;
        let (generation, mut filter, checksum): (u64, Vec<u8>, String) = tx
            .query_row(
                "SELECT generation,filter,filter_checksum FROM metadata WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(|e| e.to_string())?;
        if filter.len() != FILTER_BYTES
            || checksum != format!("{:016x}", persistence_checksum(&filter))
        {
            return Err("history archive filter length changed".into());
        }
        for (key, row) in &rows.trades {
            let mut value = row.clone();
            value.retired_at_ms = 0;
            store_row(&tx, 1, key, base_trade_key(key), &value)?;
            add_filter(&mut filter, 1, base_trade_key(key));
        }
        for (key, row) in &rows.orders {
            let mut value = row.clone();
            value.audited_at_ms = 0;
            store_row(&tx, 2, key, key, &value)?;
            add_filter(&mut filter, 2, key);
        }
        let generation = generation
            .checked_add(1)
            .ok_or("history archive generation exhausted")?;
        tx.execute(
            "UPDATE metadata SET generation=?1,filter=?2,filter_checksum=?3 WHERE singleton=1",
            params![
                generation,
                filter,
                format!("{:016x}", persistence_checksum(&filter))
            ],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        self.generation.store(generation, Ordering::SeqCst);
        for (key, _) in &rows.trades {
            for bit in filter_indices(1, base_trade_key(key)) {
                self.filter[bit / 8].fetch_or(1 << (bit % 8), Ordering::Release);
            }
        }
        for (key, _) in &rows.orders {
            for bit in filter_indices(2, key) {
                self.filter[bit / 8].fetch_or(1 << (bit % 8), Ordering::Release);
            }
        }
        Ok(generation)
    }

    /// Bounded exact-key cold lookup. A venue trade can have multiple owned
    /// maker legs. No historical payloads are read at normal process startup.
    pub fn load(&self, kind: u8, key: &str, now_ms: u64) -> Result<ArchiveRows, String> {
        if !self.may_contain(kind, key) {
            return Ok(ArchiveRows::default());
        }
        let connection = self.connection(false)?;
        self.load_with_connection(&connection, kind, key, now_ms)
    }

    pub(super) fn load_with_connection(
        &self,
        connection: &Connection,
        kind: u8,
        key: &str,
        now_ms: u64,
    ) -> Result<ArchiveRows, String> {
        if !self.may_contain(kind, key) {
            return Ok(ArchiveRows::default());
        }
        let mut query = connection
            .prepare(
                "SELECT key,payload,checksum FROM proof INDEXED BY proof_base WHERE kind=?1 AND base_key=?2 LIMIT ?3",
            )
            .map_err(|e| e.to_string())?;
        let mut result = query
            .query(params![kind, key, MAX_LOOKUP_ROWS + 1])
            .map_err(|e| e.to_string())?;
        let mut rows = ArchiveRows::default();
        while let Some(row) = result.next().map_err(|e| e.to_string())? {
            if rows.len() == MAX_LOOKUP_ROWS {
                return Err("history archive lookup exceeds bounded replay batch".into());
            }
            let stored_key: String = row.get(0).map_err(|e| e.to_string())?;
            let payload: Vec<u8> = row.get(1).map_err(|e| e.to_string())?;
            let checksum: String = row.get(2).map_err(|e| e.to_string())?;
            if format!("{:016x}", persistence_checksum(&payload)) != checksum {
                return Err("history archive proof checksum mismatch".into());
            }
            match kind {
                1 => {
                    let mut proof: RetiredTradeOwnershipTombstone =
                        serde_json::from_slice(&payload).map_err(|e| e.to_string())?;
                    if proof.ownership.account_id != self.account_id
                        || proof.ownership.trade_key != stored_key
                        || base_trade_key(&stored_key) != key
                    {
                        return Err("history archive trade identity mismatch".into());
                    }
                    proof.retired_at_ms = now_ms;
                    rows.trades.push((stored_key, proof));
                }
                2 => {
                    let mut proof: RetiredOrderAuditTombstone =
                        serde_json::from_slice(&payload).map_err(|e| e.to_string())?;
                    if normalize_order_id(&proof.order_id) != stored_key || stored_key != key {
                        return Err("history archive order identity mismatch".into());
                    }
                    proof.audited_at_ms = now_ms;
                    rows.orders.push((stored_key, proof));
                }
                _ => return Err("invalid history archive proof kind".into()),
            }
        }
        Ok(rows)
    }
}

fn configure(connection: &Connection) -> Result<(), String> {
    connection
        .busy_timeout(Duration::ZERO)
        .map_err(|e| e.to_string())?;
    connection
        .execute_batch("PRAGMA synchronous=FULL; PRAGMA cache_size=-512; PRAGMA mmap_size=0;")
        .map_err(|e| e.to_string())
}
fn store_row<T: Serialize>(
    connection: &Connection,
    kind: u8,
    key: &str,
    base: &str,
    value: &T,
) -> Result<(), String> {
    let payload = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    let previous: Option<Vec<u8>> = connection
        .query_row(
            "SELECT payload FROM proof WHERE kind=?1 AND key=?2",
            params![kind, key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    if let Some(previous) = previous {
        if previous != payload {
            return Err(format!(
                "history archive immutable proof changed kind={kind} key={key}"
            ));
        }
        return Ok(());
    }
    let checksum = format!("{:016x}", persistence_checksum(&payload));
    connection
        .execute(
            "INSERT INTO proof VALUES (?1,?2,?3,?4,?5)",
            params![kind, key, base, payload, checksum],
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn zero_settled_token(state: &SharedAccountState, token: &str) -> bool {
    state.settled_token_values.contains_key(token)
        && state.physical_positions.get(token).copied().unwrap_or(0.0) == 0.0
        && state.instances.values().all(|instance| {
            instance.positions.get(token).copied().unwrap_or(0.0) == 0.0
                && instance
                    .reserved_positions
                    .get(token)
                    .copied()
                    .unwrap_or(0.0)
                    == 0.0
                && instance
                    .maintenance_reserved_positions
                    .get(token)
                    .copied()
                    .unwrap_or(0.0)
                    == 0.0
        })
}
fn trade_eligible(
    state: &SharedAccountState,
    key: &str,
    row: &RetiredTradeOwnershipTombstone,
    now_ms: u64,
) -> bool {
    row.retired_at_ms != 0 && now_ms.saturating_sub(row.retired_at_ms) >= HOT_RETENTION_MS
        && matches!(row.ownership.status.as_str(), "CONFIRMED" | "FAILED")
        && zero_settled_token(state, &row.ownership.token_id)
        // Surviving order fill derivation and terminal audits still need the
        // exact hot proof; those roots must be retired together in a later GC.
        && !state.orders.contains_key(&row.ownership.client_order_id)
        && !state.oid_to_coid.contains_key(&normalize_order_id(&row.ownership.order_id))
        && !state.trades.contains_key(key)
        && !state.unresolved_trade_match_times.contains_key(key)
        && !state.ownership_anomalies.contains_key(&format!("trade:{key}"))
        && !state.ownership_anomalies.contains_key(&format!("private_event:trade:{}", base_trade_key(key)))
}
fn order_eligible(
    state: &SharedAccountState,
    key: &str,
    row: &RetiredOrderAuditTombstone,
    now_ms: u64,
) -> bool {
    row.audited_at_ms != 0
        && now_ms.saturating_sub(row.audited_at_ms) >= HOT_RETENTION_MS
        && !state.oid_to_coid.contains_key(key)
        && row
            .client_order_id
            .as_ref()
            .is_none_or(|coid| !state.orders.contains_key(coid))
        && !state
            .ownership_anomalies
            .contains_key(&format!("private_event:order:{key}"))
}
fn candidates(state: &SharedAccountState, now_ms: u64, limit: usize) -> ArchiveRows {
    let trades = state
        .retired_trade_ownership_tombstones
        .iter()
        .filter(|(key, row)| trade_eligible(state, key, row, now_ms))
        .take(limit)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect::<Vec<_>>();
    let orders = state
        .retired_order_audit_tombstones
        .iter()
        .filter(|(key, row)| order_eligible(state, key, row, now_ms))
        .take(limit.saturating_sub(trades.len()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    ArchiveRows { trades, orders }
}

pub(super) fn startup_archive(
    archive: &HistoryArchive,
    state: &mut SharedAccountState,
    now_ms: u64,
) -> Result<usize, String> {
    let rows = candidates(state, now_ms, usize::MAX);
    if rows.is_empty() {
        state.history_archive_generation = archive
            .connection(false)?
            .query_row(
                "SELECT generation FROM metadata WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        return Ok(0);
    }
    let generation = archive.store(&rows)?;
    for (key, _) in &rows.trades {
        state.retired_trade_ownership_tombstones.remove(key);
    }
    for (key, _) in &rows.orders {
        state.retired_order_audit_tombstones.remove(key);
    }
    // Dropping rows alone retains the original historical bucket allocation.
    state.retired_trade_ownership_tombstones.shrink_to_fit();
    state.retired_order_audit_tombstones.shrink_to_fit();
    state.history_archive_generation = generation;
    Ok(rows.len())
}

impl SharedAccount {
    /// Fixed-memory advisory membership only. Read by the private route; a hit
    /// defers the whole event to the existing cold account worker for proof.
    pub fn archived_private_event_hint(&self, trade: bool, identity: &str) -> bool {
        self.history_archive
            .as_ref()
            .is_some_and(|archive| archive.may_contain(if trade { 1 } else { 2 }, identity))
    }

    // Reserve the existing bounded cold-reclamation credits before any hot
    // mutation. One changed route produces at most one obsolete shard snapshot.
    fn reserve_archive_route_retirement(
        &self,
        rows: usize,
    ) -> Result<Vec<RouteRetirementPermit<'_>>, String> {
        if !self.account_owner_lane_bound.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        (0..rows.div_ceil(route_retirement::SNAPSHOTS_PER_BATCH))
            .map(|_| {
                self.route_retirement
                    .try_reserve()
                    .ok_or_else(|| "archive route reclamation capacity busy".to_owned())
            })
            .collect()
    }

    pub(super) fn archive_retired_history(&self, now_ms: u64) -> Result<usize, String> {
        assert!(
            !self.account_owner_lane_bound.load(Ordering::Acquire)
                || self.is_account_owner_thread(),
            "history archive writes belong to the cold owner"
        );
        let Some(archive) = &self.history_archive else {
            return Ok(0);
        };
        if self
            .lifecycle_mirror_published_watermark
            .load(Ordering::Acquire)
            != self
                .lifecycle_mirror_applied_watermark
                .load(Ordering::Acquire)
        {
            return Ok(0);
        }
        let rows = {
            let Ok(state) = self.state.try_lock() else {
                return Ok(0);
            };
            candidates(&state, now_ms, 128)
        };
        if rows.is_empty() {
            return Ok(0);
        }
        let started = crate::latency::Instant::now();
        let generation = archive.store(&rows)?;
        // Disk I/O above never holds the account control/state locks. Another
        // lifecycle can overtake the write; revalidate exact rows before removal.
        let Ok(_control) = self.control_gate.try_write() else {
            return Ok(0);
        };
        let Ok(mut state) = self.state.try_lock() else {
            return Ok(0);
        };
        if self
            .lifecycle_mirror_published_watermark
            .load(Ordering::Acquire)
            != self
                .lifecycle_mirror_applied_watermark
                .load(Ordering::Acquire)
        {
            return Ok(0);
        }
        let Ok(mut retirement) = self.reserve_archive_route_retirement(rows.trades.len()) else {
            return Ok(0); // durable duplicate is safe; retain every hot proof
        };
        let mut changes = Vec::new();
        let mut retired = 0;
        for (index, (key, row)) in rows.trades.into_iter().enumerate() {
            if state.retired_trade_ownership_tombstones.get(&key) == Some(&row)
                && trade_eligible(&state, &key, &row, now_ms)
            {
                state.retired_trade_ownership_tombstones.remove(&key);
                self.retired_trade_routes.apply_batch_retiring(
                    &row.ownership.instance_id,
                    std::slice::from_ref(&key),
                    &[],
                    retirement.get_mut(index / route_retirement::SNAPSHOTS_PER_BATCH),
                );
                changes.push(PersistenceWalChange::Remove {
                    path: vec!["retired_trade_ownership_tombstones".into(), key],
                });
                retired += 1;
            }
        }
        let mut orders_changed = false;
        for (key, row) in rows.orders {
            if state.retired_order_audit_tombstones.get(&key) == Some(&row)
                && order_eligible(&state, &key, &row, now_ms)
            {
                state.retired_order_audit_tombstones.remove(&key);
                changes.push(PersistenceWalChange::Remove {
                    path: vec!["retired_order_audit_tombstones".into(), key],
                });
                retired += 1;
                orders_changed = true;
            }
        }
        if retired != 0 {
            state.history_archive_generation = generation;
            persistence_wal_set(
                &mut changes,
                ["history_archive_generation".to_owned()],
                &generation,
            )?;
            self.persistence
                .as_ref()
                .ok_or("history archive requires durable ledger")?
                .schedule_delta(changes);
            self.retired_trade_tombstone_count_fast.store(
                state.retired_trade_ownership_tombstones.len(),
                Ordering::Relaxed,
            );
            if orders_changed {
                self.publish_retired_order_audit_tombstones(&state);
            }
            log::info!("[account_history_archive] account={} boundary=cold_owner hot_retention_ms={} archived={} trade_hot={} order_audit_hot={} generation={}", self.account_id, HOT_RETENTION_MS, retired, state.retired_trade_ownership_tombstones.len(),state.retired_order_audit_tombstones.len(),generation);
        }
        drop(state);
        drop(_control);
        crate::latency::record("polymarket.account.history_archive.cold_commit", started);
        Ok(retired)
    }

    /// Cold-owner-only I/O. Restored rows contain no bookable economics; the
    /// original strict lifecycle validator must still authenticate the event.
    #[cfg(test)]
    pub(super) fn hydrate_archived_private_event(
        &self,
        trade: bool,
        identity: &str,
    ) -> Result<usize, String> {
        self.hydrate_archived_private_event_with_connection(trade, identity, None)
    }

    pub(super) fn hydrate_archived_private_event_with_connection(
        &self,
        trade: bool,
        identity: &str,
        connection: Option<&Connection>,
    ) -> Result<usize, String> {
        assert!(
            !self.account_owner_lane_bound.load(Ordering::Acquire)
                || self.is_account_owner_thread(),
            "history archive reads belong to the cold owner"
        );
        let Some(archive) = &self.history_archive else {
            return Ok(0);
        };
        let started = crate::latency::Instant::now();
        let kind = if trade { 1 } else { 2 };
        let rows = if let Some(connection) = connection {
            archive.load_with_connection(connection, kind, identity, wall_clock_ms())?
        } else {
            archive.load(kind, identity, wall_clock_ms())?
        };
        if rows.is_empty() {
            if trade {
                archive.record_verified_absence(identity)?;
            }
            return Ok(0);
        }
        let _control = self.control_gate.write().unwrap();
        let mut state = self.state.lock().unwrap();
        let mut changes = Vec::new();
        let mut restored = 0;
        // Validate the complete bounded batch before publishing any hydration.
        for (key, row) in &rows.trades {
            if !state.instances.contains_key(&row.ownership.instance_id) {
                return Err(format!("archived trade owner is missing: {key}"));
            }
            if state.trades.contains_key(key) {
                return Err(format!(
                    "archived trade overlaps a live economic row: {key}"
                ));
            }
            if let Some(current) = state.retired_trade_ownership_tombstones.get(key) {
                let mut current = current.clone();
                current.retired_at_ms = row.retired_at_ms;
                if current != *row {
                    return Err(format!("archived trade conflicts with hot proof: {key}"));
                }
            }
        }
        for (key, row) in &rows.orders {
            if let Some(current) = state.retired_order_audit_tombstones.get(key) {
                let mut current = current.clone();
                current.audited_at_ms = row.audited_at_ms;
                if current != *row {
                    return Err(format!("archived order conflicts with hot proof: {key}"));
                }
            }
        }
        let mut retirement = self.reserve_archive_route_retirement(rows.trades.len())?;
        for (index, (key, row)) in rows.trades.into_iter().enumerate() {
            // Refresh verified proofs as well: a replay can arrive after the
            // legacy hot TTL, while its exact archive proof remains permanent.
            self.retired_trade_routes.apply_batch_retiring(
                &row.ownership.instance_id,
                &[],
                &[(key.clone(), row.ownership.instance_id.clone())],
                retirement.get_mut(index / route_retirement::SNAPSHOTS_PER_BATCH),
            );
            persistence_wal_map_entry(
                &mut changes,
                "retired_trade_ownership_tombstones",
                &key,
                Some(&row),
            )?;
            state.retired_trade_ownership_tombstones.insert(key, row);
            restored += 1;
        }
        let mut orders_changed = false;
        for (key, row) in rows.orders {
            persistence_wal_map_entry(
                &mut changes,
                "retired_order_audit_tombstones",
                &key,
                Some(&row),
            )?;
            state.retired_order_audit_tombstones.insert(key, row);
            orders_changed = true;
            restored += 1;
        }
        if restored != 0 {
            self.persistence
                .as_ref()
                .ok_or("archive hydration requires durable ledger")?
                .schedule_delta(changes);
            self.retired_trade_tombstone_count_fast.store(
                state.retired_trade_ownership_tombstones.len(),
                Ordering::Relaxed,
            );
            if orders_changed {
                self.publish_retired_order_audit_tombstones(&state);
            }
        }
        drop(state);
        drop(_control);
        crate::latency::record("polymarket.account.history_archive.cold_lookup", started);
        Ok(restored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "hexagent-history-archive-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir.join("account.json"))
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(self.0.parent().unwrap());
        }
    }
    fn proof(key: &str, retired_at_ms: u64) -> RetiredTradeOwnershipTombstone {
        RetiredTradeOwnershipTombstone {
            ownership: TradeOwnership {
                order_slot: Default::default(),
                account_id: "account".into(),
                instance_id: "owner".into(),
                trade_key: key.into(),
                client_order_id: "old-coid".into(),
                order_id: "old-oid".into(),
                token_id: "TOKEN".into(),
                side: Side::Buy,
                quantity: 2.0,
                price: 0.5,
                status: "CONFIRMED".into(),
            },
            execution_pricing: None,
            is_maker: Some(true),
            authenticated_terminal_noop: true,
            retired_at_ms,
        }
    }
    fn state(now: u64) -> SharedAccountState {
        let account = SharedAccount::new("account");
        account.register_instance("owner", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        let mut state = account.lock_state().state.clone();
        state.settled_token_values.insert("TOKEN".into(), 1.0);
        state.retired_trade_ownership_tombstones.insert(
            "old-trade".into(),
            proof("old-trade", now - HOT_RETENTION_MS),
        );
        state.retired_trade_ownership_tombstones.insert(
            "recent-trade".into(),
            proof("recent-trade", now - HOT_RETENTION_MS + 1),
        );
        state
    }
    #[test]
    fn bounded_exact_absence_cannot_authorize_a_different_or_newly_archived_trade() {
        let fixture = Fixture::new();
        let archive = HistoryArchive::open(&fixture.0, "account", 0).unwrap();
        // Force real advisory positives without adding any exact disk proof.
        for byte in &archive.filter {
            byte.store(u8::MAX, Ordering::Release);
        }
        let key = "new-trade";
        assert!(archive.unverified_trade_hint(key));
        assert!(archive.load(1, key, 1).unwrap().is_empty());
        archive.record_verified_absence(key).unwrap();
        assert!(!archive.unverified_trade_hint("new-trade:maker-leg"));
        assert!(archive.unverified_trade_hint("new-trade-other"));
        let other = Fixture::new();
        let other_archive = HistoryArchive::open(&other.0, "other", 0).unwrap();
        for byte in &other_archive.filter {
            byte.store(u8::MAX, Ordering::Release);
        }
        assert!(other_archive.unverified_trade_hint(key));
        assert!(archive
            .record_verified_absence(&"x".repeat(ABSENCE_KEY_BYTES + 1))
            .is_err());
        // Readers refuse an in-progress slot replacement even with matching bytes.
        archive.absences[0].sequence.fetch_add(1, Ordering::SeqCst);
        assert!(archive.unverified_trade_hint(key));
        archive.absences[0].sequence.fetch_add(1, Ordering::SeqCst);
        for index in 0..ABSENCE_SLOTS {
            archive
                .record_verified_absence(&format!("absent-{index}"))
                .unwrap();
        }
        assert!(
            archive.unverified_trade_hint(key),
            "FIFO eviction is bounded and fail closed"
        );
        archive.record_verified_absence(key).unwrap();
        assert!(!archive.unverified_trade_hint(key));
        archive
            .store(&ArchiveRows {
                trades: vec![(key.into(), proof(key, 1))],
                orders: vec![],
            })
            .unwrap();
        assert!(
            archive.unverified_trade_hint(key),
            "durable append invalidates old absence"
        );
    }

    #[test]
    fn sdk_entry_rejects_archived_trade_even_with_reconstructed_parent() {
        let fixture = Fixture::new();
        let account = SharedAccount::new_persistent("account", &fixture.0).unwrap();
        account.register_instance("owner", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        account
            .reserve_order(
                "owner",
                "old-coid",
                "old-oid",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                0,
            )
            .unwrap();
        let archive = account.history_archive.as_ref().unwrap();
        archive
            .store(&ArchiveRows {
                trades: vec![("old-trade".into(), proof("old-trade", 1))],
                orders: vec![],
            })
            .unwrap();
        let before = account.instance_snapshot("owner").unwrap();
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "old-trade",
                "CONFIRMED",
                "old-coid",
                "old-oid",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                true,
                1
            ),
            TradeTransitionResult::Rejected
        ));
        assert_eq!(
            account.instance_snapshot("owner").unwrap().cash,
            before.cash
        );
        assert_eq!(account.order("old-coid").unwrap().filled_quantity, 0.0);
        account
            .hydrate_archived_private_event(true, "old-trade")
            .unwrap();
        // Retained dependent proofs cannot silently expire at the old 90-day
        // standalone TTL and turn an old fill into a new economic event.
        account
            .state
            .lock()
            .unwrap()
            .retired_trade_ownership_tombstones
            .get_mut("old-trade")
            .unwrap()
            .retired_at_ms = 1;
        assert!(account.trade_ownership("old-trade").is_some());
        assert!(account.trade_lifecycle_covers_nonblocking(
            "old-trade",
            "CONFIRMED",
            "old-oid",
            "TOKEN",
            Side::Buy,
            2.0,
            0.5,
            true
        ));
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "old-trade",
                "CONFIRMED",
                "old-coid",
                "old-oid",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                true,
                1
            ),
            TradeTransitionResult::OwnedNoop(_)
        ));
        assert_eq!(
            account.instance_snapshot("owner").unwrap().cash,
            before.cash
        );
    }

    #[test]
    fn sdk_entry_books_real_bloom_false_positive_once_after_cold_absence() {
        let fixture = Fixture::new();
        let account = SharedAccount::new_persistent("account", &fixture.0).unwrap();
        account.register_instance("owner", 1.0);
        account
            .apply_physical_snapshot(100.0, HashMap::new())
            .unwrap();
        account
            .reserve_order(
                "owner",
                "new-coid",
                "new-oid",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                0,
            )
            .unwrap();
        let archive = account.history_archive.as_ref().unwrap();
        for byte in &archive.filter {
            byte.store(u8::MAX, Ordering::Release);
        }
        assert!(archive.unverified_trade_hint("new-trade"));
        assert_eq!(
            account
                .hydrate_archived_private_event(true, "new-trade")
                .unwrap(),
            0
        );
        assert!(!archive.unverified_trade_hint("new-trade"));
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "new-trade",
                "CONFIRMED",
                "new-coid",
                "new-oid",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                true,
                1
            ),
            TradeTransitionResult::Applied(_)
        ));
        let cash = account.instance_snapshot("owner").unwrap().cash;
        assert!(matches!(
            account.apply_trade_transition_with_context(
                "new-trade",
                "CONFIRMED",
                "new-coid",
                "new-oid",
                "TOKEN",
                Side::Buy,
                2.0,
                0.5,
                true,
                1
            ),
            TradeTransitionResult::OwnedNoop(_)
        ));
        assert_eq!(account.instance_snapshot("owner").unwrap().cash, cash);
        assert_eq!(account.order("new-coid").unwrap().filled_quantity, 2.0);
    }

    #[test]
    fn ttl_boundary_zero_inventory_dependencies_and_rollback_clock() {
        let now = HOT_RETENTION_MS * 2;
        let mut s = state(now);
        assert_eq!(candidates(&s, now, 128).trades.len(), 1);
        assert!(candidates(&s, now - HOT_RETENTION_MS, 128).is_empty());
        s.instances
            .get_mut("owner")
            .unwrap()
            .positions
            .insert("TOKEN".into(), 1e-12);
        assert!(candidates(&s, now, 128).is_empty());
        s.instances
            .get_mut("owner")
            .unwrap()
            .positions
            .insert("TOKEN".into(), 0.0);
        s.instances
            .get_mut("owner")
            .unwrap()
            .reserved_positions
            .insert("TOKEN".into(), 1e-12);
        assert!(candidates(&s, now, 128).is_empty());
        s.instances
            .get_mut("owner")
            .unwrap()
            .reserved_positions
            .clear();
        s.oid_to_coid.insert("old-oid".into(), "old-coid".into());
        assert!(candidates(&s, now, 128).is_empty());
        s.oid_to_coid.clear();
        s.unresolved_trade_match_times.insert("old-trade".into(), 1);
        assert!(candidates(&s, now, 128).is_empty());
    }
    #[test]
    fn archive_commit_is_idempotent_and_conflicts_do_not_overwrite_proof() {
        let fixture = Fixture::new();
        let archive = HistoryArchive::open(&fixture.0, "account", 0).unwrap();
        let rows = ArchiveRows {
            trades: vec![("venue:old-oid".into(), proof("venue:old-oid", 1))],
            orders: vec![],
        };
        let first = archive.store(&rows).unwrap();
        let second = archive.store(&rows).unwrap();
        assert!(second > first);
        assert!(archive.may_contain(1, "venue"));
        let restored = archive.load(1, "venue", 99).unwrap();
        assert_eq!(restored.trades.len(), 1);
        assert_eq!(restored.trades[0].1.retired_at_ms, 99);
        let mut conflict = restored;
        conflict.trades[0].1.ownership.quantity = 3.0;
        assert!(archive.store(&conflict).is_err());
        assert_eq!(
            archive.load(1, "venue", 100).unwrap().trades[0]
                .1
                .ownership
                .quantity,
            2.0
        );
        assert!(HistoryArchive::open(&fixture.0, "other-account", 0).is_err());
        assert!(HistoryArchive::open(&fixture.0, "account", second + 1).is_err());
    }
    #[test]
    fn startup_archive_restart_lazy_replay_and_missing_archive_fail_closed() {
        let fixture = Fixture::new();
        let now = wall_clock_ms();
        let mut original = state(now);
        original
            .retired_trade_ownership_tombstones
            .remove("recent-trade");
        write_persisted_account(
            &fixture.0,
            &PersistedAccount {
                version: 1,
                account_id: "account".into(),
                persistence_generation: 0,
                state: original,
            },
        )
        .unwrap();
        {
            let account = SharedAccount::new_persistent("account", &fixture.0).unwrap();
            assert!(account
                .state
                .lock()
                .unwrap()
                .retired_trade_ownership_tombstones
                .is_empty());
            assert!(account.archived_private_event_hint(true, "old-trade"));
            assert!(!account.has_private_trade_identity("old-trade"));
            assert_eq!(
                account
                    .hydrate_archived_private_event(true, "old-trade")
                    .unwrap(),
                1
            );
            let before = account.monitoring_snapshot();
            for status in ["MATCHED", "CONFIRMED", "CONFIRMED"] {
                assert!(matches!(
                    account.apply_trade_transition_with_context(
                        "old-trade",
                        status,
                        "",
                        "old-oid",
                        "TOKEN",
                        Side::Buy,
                        2.0,
                        0.5,
                        true,
                        1
                    ),
                    TradeTransitionResult::OwnedNoop(_)
                ));
            }
            assert_eq!(
                account.monitoring_snapshot().physical_cash,
                before.physical_cash
            );
            account.flush_persistence(Duration::from_secs(2)).unwrap();
        }
        {
            let account = SharedAccount::new_persistent("account", &fixture.0).unwrap();
            assert_eq!(account.trade_ownership("old-trade").unwrap().quantity, 2.0);
            assert!(matches!(
                account.apply_trade_transition_with_context(
                    "old-trade",
                    "CONFIRMED",
                    "",
                    "old-oid",
                    "TOKEN",
                    Side::Buy,
                    3.0,
                    0.5,
                    true,
                    1
                ),
                TradeTransitionResult::Rejected
            ));
        }
        let mut path = fixture.0.as_os_str().to_os_string();
        path.push(".history.sqlite3");
        std::fs::remove_file(PathBuf::from(path)).unwrap();
        assert!(SharedAccount::new_persistent("account", &fixture.0)
            .unwrap_err()
            .contains("archive missing"));
    }
    #[test]
    fn archive_route_reclamation_retains_readers_and_backpressures_before_hot_mutation() {
        let fixture = Fixture::new();
        let account = Arc::new(SharedAccount::new_persistent("account", &fixture.0).unwrap());
        let now = wall_clock_ms();
        {
            let mut target = account.state.lock().unwrap();
            *target = state(now);
            target.history_archive_generation = 1;
            target
                .retired_trade_ownership_tombstones
                .remove("recent-trade");
        }
        account
            .retired_trade_routes
            .insert("old-trade".into(), "owner".into());
        let (_, cold) = account.bind_account_owner().unwrap();
        cold.mark_current_thread().unwrap();
        let old_reader = account.retired_trade_routes.shards
            [ShardedRouteMap::shard_index("old-trade")]
        .published
        .load();
        let mut credits = Vec::new();
        while let Some(credit) = account.route_retirement.try_reserve() {
            credits.push(credit);
        }
        assert_eq!(account.archive_retired_history(now).unwrap(), 0);
        assert!(account
            .state
            .lock()
            .unwrap()
            .retired_trade_ownership_tombstones
            .contains_key("old-trade"));
        assert!(account.retired_trade_routes.contains("old-trade"));
        drop(credits);
        assert_eq!(account.archive_retired_history(now).unwrap(), 1);
        assert!(!account.retired_trade_routes.contains("old-trade"));
        cold.reclaim_retired_routes();
        assert_eq!(account.route_retirement_metrics().0, 1);
        assert_eq!(old_reader.get("old-trade").unwrap().as_ref(), "owner");
        drop(old_reader);
        cold.reclaim_retired_routes();
        assert_eq!(account.route_retirement_metrics().0, 0);
        let mut credits = Vec::new();
        while let Some(credit) = account.route_retirement.try_reserve() {
            credits.push(credit);
        }
        assert!(cold
            .hydrate_archived_private_event(true, "old-trade")
            .unwrap_err()
            .contains("capacity busy"));
        assert!(!account.retired_trade_routes.contains("old-trade"));
        assert!(account
            .state
            .lock()
            .unwrap()
            .retired_trade_ownership_tombstones
            .is_empty());
        drop(credits);
        assert_eq!(
            cold.hydrate_archived_private_event(true, "old-trade")
                .unwrap(),
            1
        );
        assert!(account.retired_trade_routes.contains("old-trade"));
        cold.reclaim_retired_routes();
    }

    #[test]
    fn cold_removal_is_bounded_and_crash_before_eviction_keeps_both_sources_safe() {
        let fixture = Fixture::new();
        let account = SharedAccount::new_persistent("account", &fixture.0).unwrap();
        let now = wall_clock_ms();
        {
            let mut target = account.state.lock().unwrap();
            *target = state(now);
            target.history_archive_generation = 1;
            target.retired_trade_ownership_tombstones.clear();
            for i in 0..300 {
                let key = format!("trade-{i}");
                target
                    .retired_trade_ownership_tombstones
                    .insert(key.clone(), proof(&key, now - HOT_RETENTION_MS));
            }
        }
        let rows = candidates(&account.state.lock().unwrap(), now, 128);
        account
            .history_archive
            .as_ref()
            .unwrap()
            .store(&rows)
            .unwrap(); // crash boundary: ledger untouched
        assert_eq!(
            account
                .state
                .lock()
                .unwrap()
                .retired_trade_ownership_tombstones
                .len(),
            300
        );
        assert_eq!(account.archive_retired_history(now).unwrap(), 128);
        assert_eq!(account.archive_retired_history(now).unwrap(), 128);
        assert_eq!(account.archive_retired_history(now).unwrap(), 44);
        assert!(account
            .state
            .lock()
            .unwrap()
            .retired_trade_ownership_tombstones
            .is_empty());
    }
    #[test]
    fn order_filter_normalization_and_cold_exact_identity() {
        let fixture = Fixture::new();
        let archive = HistoryArchive::open(&fixture.0, "account", 0).unwrap();
        let proof = RetiredOrderAuditTombstone {
            order_id: "abcdef".into(),
            client_order_id: None,
            status: OrderStatus::Cancelled,
            original_size: 2.0,
            covers_any_zero_fill_size: false,
            size_matched: 0.0,
            associate_trades: vec![],
            evidence: "authenticated query".into(),
            audited_at_ms: 1,
        };
        archive
            .store(&ArchiveRows {
                trades: vec![],
                orders: vec![("abcdef".into(), proof)],
            })
            .unwrap();
        assert!(archive.may_contain(2, " 0XAbCdEf "));
        assert_eq!(archive.load(2, "abcdef", 100).unwrap().orders.len(), 1);
        assert!(archive.load(2, "other", 100).unwrap().is_empty());
    }
    #[test]
    fn corrupted_payload_and_read_failure_never_authorize_replay() {
        let fixture = Fixture::new();
        let archive = HistoryArchive::open(&fixture.0, "account", 0).unwrap();
        archive
            .store(&ArchiveRows {
                trades: vec![("old-trade".into(), proof("old-trade", 1))],
                orders: vec![],
            })
            .unwrap();
        archive
            .connection(true)
            .unwrap()
            .execute("UPDATE proof SET payload='{}'", [])
            .unwrap();
        assert!(archive.load(1, "old-trade", 100).is_err());
        std::fs::remove_file(&archive.path).unwrap();
        assert!(archive.load(1, "old-trade", 100).is_err());
    }
    #[test]
    #[ignore = "requires a copied maker02 checkpoint/WAL fixture directory; never opens live files"]
    fn maker02_archive_migration_and_lookup_benchmark() {
        let root = PathBuf::from(std::env::var("HEXBOT_ARCHIVE_FIXTURES").unwrap());
        for name in ["hex001", "zhu03", "zhu02"] {
            let fixture = Fixture::new();
            std::fs::copy(root.join(format!("{name}.json")), &fixture.0).unwrap();
            let wal = root.join(format!("{name}.json.wal"));
            if wal.exists() {
                std::fs::copy(wal, persistence_wal_path(&fixture.0)).unwrap();
            }
            let started = Instant::now();
            let account = SharedAccount::new_persistent(name, &fixture.0).unwrap();
            let first_open_ms = started.elapsed().as_millis();
            let state = account.state.lock().unwrap();
            validate_persisted_state(name, &state).unwrap();
            let hot_trades = state.retired_trade_ownership_tombstones.len();
            let hot_orders = state.retired_order_audit_tombstones.len();
            let serialized_hot_bytes = serde_json::to_vec(&*state).unwrap().len();
            let archive = account.history_archive.as_ref().unwrap();
            let connection = archive.connection(false).unwrap();
            let archived: usize = connection
                .query_row("SELECT COUNT(*) FROM proof", [], |r| r.get(0))
                .unwrap();
            let key: String = connection
                .query_row("SELECT base_key FROM proof WHERE kind=1 LIMIT 1", [], |r| {
                    r.get(0)
                })
                .unwrap();
            drop(connection);
            drop(state);
            let mut samples = Vec::with_capacity(10000);
            for _ in 0..10000 {
                let start = Instant::now();
                assert!(std::hint::black_box(
                    account.archived_private_event_hint(true, std::hint::black_box(&key))
                ));
                samples.push(start.elapsed().as_nanos() as u64);
            }
            samples.sort_unstable();
            println!("archive_hint account={name} n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=fixed_filter_lookup_only filter_bytes={FILTER_BYTES}",samples.len(),samples[4999],samples[9899],samples[9989],samples[9999]);
            for (label, identity, expected) in [
                ("positive", key.as_str(), true),
                ("negative", "absent-benchmark-trade", false),
            ] {
                let mut samples = Vec::with_capacity(10000);
                for _ in 0..10000 {
                    let start = Instant::now();
                    assert_eq!(
                        std::hint::black_box(
                            archive.unverified_trade_hint(std::hint::black_box(identity))
                        ),
                        expected
                    );
                    samples.push(start.elapsed().as_nanos() as u64);
                }
                samples.sort_unstable();
                println!("archive_sdk_guard account={name} case={label} n=10000 p50_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=filter+bounded_exact_absence_guard",samples[4999],samples[9899],samples[9989],samples[9999]);
            }
            let reader = archive.connection(false).unwrap();
            let mut samples = Vec::with_capacity(100);
            for _ in 0..100 {
                let start = Instant::now();
                assert!(!archive
                    .load_with_connection(&reader, 1, &key, wall_clock_ms())
                    .unwrap()
                    .is_empty());
                samples.push(start.elapsed().as_nanos() as u64);
            }
            samples.sort_unstable();
            println!("archive_disk account={name} n={} p50_ns={} p99_ns={} p999_ns={} max_ns={} in_flight=1 ending_depth=0 overflow=0 boundary=cached_cold_connection+bounded_lookup+checksum_decode excludes=first_connection_open_and_message_queue",samples.len(),samples[49],samples[98],samples[99],samples[99]);
            account.flush_persistence(Duration::from_secs(5)).unwrap();
            drop(account);
            let started = Instant::now();
            let reopened = SharedAccount::new_persistent(name, &fixture.0).unwrap();
            let second_open_ms = started.elapsed().as_millis();
            assert_eq!(
                reopened
                    .state
                    .lock()
                    .unwrap()
                    .retired_trade_ownership_tombstones
                    .len(),
                hot_trades
            );
            println!("archive_migration account={name} archived={archived} hot_trades={hot_trades} hot_order_audits={hot_orders} serialized_hot_bytes={serialized_hot_bytes} first_open_ms={first_open_ms} second_open_ms={second_open_ms} checkpoint_and_WAL=validated economics=unchanged restart=passed");
        }
    }
    #[test]
    fn lookup_uses_base_index_and_filter_corruption_fails_startup() {
        let fixture = Fixture::new();
        let archive = HistoryArchive::open(&fixture.0, "account", 0).unwrap();
        archive
            .store(&ArchiveRows {
                trades: vec![("old-trade".into(), proof("old-trade", 1))],
                orders: vec![],
            })
            .unwrap();
        let connection = archive.connection(true).unwrap();
        let detail:String=connection.query_row("EXPLAIN QUERY PLAN SELECT key,payload,checksum FROM proof INDEXED BY proof_base WHERE kind=?1 AND base_key=?2 LIMIT ?3",params![1,"old-trade",129],|r|r.get(3)).unwrap();
        assert!(detail.contains("proof_base"), "{detail}");
        connection
            .execute(
                "UPDATE metadata SET filter=zeroblob(?1)",
                params![FILTER_BYTES],
            )
            .unwrap();
        assert!(HistoryArchive::open(&fixture.0, "account", 1).is_err());
        assert!(archive.store(&ArchiveRows::default()).is_err());
    }
}
