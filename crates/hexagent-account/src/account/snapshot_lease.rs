//! Account-local cold snapshot coordination. No global registry or shared lock.
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Immutable request identity; only the matching account/instance/generation
/// may renew or release it. Not a trading admission or economic risk gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotLeaseTicket {
    account_id: Arc<str>,
    instance_id: Arc<str>,
    generation: u64,
}

impl SnapshotLeaseTicket {
    pub fn account_id(&self) -> &str { &self.account_id }
}

struct Lease {
    ticket: SnapshotLeaseTicket,
    expires: Instant,
    finished: bool,
}

#[derive(Default)]
pub(super) struct SnapshotLeaseOwner {
    generation: u64,
    lease: Option<Lease>,
}

impl SnapshotLeaseOwner {
    pub(super) fn claim(
        &mut self, account: &str, instance: &str, ttl: Duration, now: Instant,
    ) -> Result<Option<SnapshotLeaseTicket>, String> {
        if account.is_empty() || instance.is_empty() || ttl.is_zero() || ttl > Duration::from_secs(3600) {
            return Err("invalid snapshot lease identity or duration".into());
        }
        if let Some(lease) = &mut self.lease {
            if now < lease.expires {
                if lease.ticket.account_id.as_ref() != account || lease.ticket.instance_id.as_ref() != instance {
                    return Ok(None);
                }
                // Same holder starts its next poll; allocate a new request
                // generation so a late finish from the prior poll cannot win.
            }
        }
        self.generation = self.generation.checked_add(1)
            .ok_or_else(|| "snapshot lease generation exhausted".to_string())?;
        let ticket = SnapshotLeaseTicket {
            account_id: account.into(), instance_id: instance.into(), generation: self.generation,
        };
        self.lease = Some(Lease { ticket: ticket.clone(), expires: now + ttl, finished: false });
        Ok(Some(ticket))
    }

    pub(super) fn finish(&mut self, ticket: &SnapshotLeaseTicket, retain: Option<Duration>, now: Instant) {
        let Some(lease) = &mut self.lease else { return; };
        if &lease.ticket != ticket || lease.finished { return; }
        match retain {
            Some(ttl) if !ttl.is_zero() && ttl <= Duration::from_secs(3600) && now < lease.expires => {
                lease.expires = now + ttl;
                lease.finished = true;
            }
            _ => self.lease = None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_expiry_and_stale_duplicate_finishes_preserve_new_owner() {
        let mut owner = SnapshotLeaseOwner::default();
        let now = Instant::now();
        let ttl = Duration::from_secs(45);
        let first = owner.claim("a", "one", ttl, now).unwrap().unwrap();
        assert!(owner.claim("a", "two", ttl, now).unwrap().is_none());
        let second = owner.claim("a", "two", ttl, now + ttl).unwrap().unwrap();
        owner.finish(&first, None, now + ttl);
        owner.finish(&first, Some(ttl), now + ttl);
        assert!(owner.claim("a", "one", ttl, now + ttl).unwrap().is_none());
        owner.finish(&second, None, now + ttl);
        owner.finish(&second, None, now + ttl);
        assert!(owner.claim("a", "one", ttl, now + ttl).unwrap().is_some());
    }

    #[test]
    fn same_instance_new_request_fences_old_release_and_accounts_are_isolated() {
        let mut a = SnapshotLeaseOwner::default();
        let mut b = SnapshotLeaseOwner::default();
        let now = Instant::now();
        let ttl = Duration::from_secs(45);
        let first = a.claim("a", "one", ttl, now).unwrap().unwrap();
        let second = a.claim("a", "one", ttl, now).unwrap().unwrap();
        let other = b.claim("b", "one", ttl, now).unwrap().unwrap();
        a.finish(&first, None, now);
        a.finish(&other, None, now);
        assert!(a.claim("a", "two", ttl, now).unwrap().is_none());
        a.finish(&second, Some(Duration::from_secs(120)), now);
        a.finish(&second, Some(Duration::from_secs(120)), now + Duration::from_secs(100));
        assert!(a.claim("a", "two", ttl, now + ttl).unwrap().is_none());
        assert!(a.claim("a", "two", ttl, now + Duration::from_secs(120)).unwrap().is_some());
    }

    #[test]
    fn lease_messages_do_not_borrow_aggregate_lock_and_recover_after_owner_queue_full() {
        use super::super::{AccountOwnerCommand, SharedAccount};
        let account = Arc::new(SharedAccount::new("lease-message-account"));
        let (handle, owner) = account.bind_account_owner().unwrap();
        let held = account.state.lock().unwrap();
        let rx = handle.submit_snapshot_lease_claim("one".into(), Duration::from_secs(45)).unwrap();
        owner.execute(owner.receiver().try_recv().unwrap());
        let ticket = rx.try_recv().unwrap().unwrap().unwrap();
        let denied = handle.submit_snapshot_lease_claim("two".into(), Duration::from_secs(45)).unwrap();
        owner.execute(owner.receiver().try_recv().unwrap());
        assert!(denied.try_recv().unwrap().unwrap().is_none());
        handle.submit_snapshot_lease_finish(ticket, None).unwrap();
        owner.execute(owner.receiver().try_recv().unwrap());
        let (barrier, _keep_reply) = crossbeam_channel::bounded(1);
        let capacity = owner.receiver().capacity().unwrap();
        for _ in 0..capacity { handle.try_send(AccountOwnerCommand::barrier(barrier.clone())).unwrap(); }
        assert!(handle.submit_snapshot_lease_claim("two".into(), Duration::from_secs(45)).is_err());
        // Discard the test barriers rather than blocking on their replies.
        while owner.receiver().try_recv().is_ok() {}
        let retry = handle.submit_snapshot_lease_claim("two".into(), Duration::from_secs(45)).unwrap();
        owner.execute(owner.receiver().try_recv().unwrap());
        assert!(retry.try_recv().unwrap().unwrap().is_some());
        drop(held);
    }

    #[test]
    fn abandoned_claim_does_not_hold_a_lease_and_sidecar_full_returns_exact_payload() {
        use super::super::{AccountOwnerCommand, DurableSidecarCheckpoint, SharedAccount};
        let account = Arc::new(SharedAccount::new("lease-abandoned-account"));
        let (handle, owner) = account.bind_account_owner().unwrap();
        drop(handle.submit_snapshot_lease_claim("one".into(), Duration::from_secs(45)).unwrap());
        owner.execute(owner.receiver().try_recv().unwrap());
        let retry = handle.submit_snapshot_lease_claim("two".into(), Duration::from_secs(45)).unwrap();
        owner.execute(owner.receiver().try_recv().unwrap());
        assert!(retry.try_recv().unwrap().unwrap().is_some());
        let (barrier, _keep_reply) = crossbeam_channel::bounded(1);
        for _ in 0..owner.receiver().capacity().unwrap() {
            handle.try_send(AccountOwnerCommand::barrier(barrier.clone())).unwrap();
        }
        let payload = "x".repeat(256 * 1024);
        let ptr = payload.as_ptr();
        let (returned, _) = handle.try_submit_sidecar_checkpoint("two".into(), DurableSidecarCheckpoint {
            generation: 7, expected_entries: 3, recovery_payload: payload,
        }).unwrap_err();
        assert_eq!(returned.recovery_payload.as_ptr(), ptr);
        assert_eq!((returned.generation, returned.expected_entries), (7, 3));
    }

    #[test]
    #[ignore = "focused cold owner message benchmark; no network or quote work"]
    fn snapshot_lease_message_benchmark() {
        use super::super::SharedAccount;
        const N: usize = 10_000;
        let account = Arc::new(SharedAccount::new("lease-benchmark"));
        let (handle, owner) = account.bind_account_owner().unwrap();
        let mut enqueue = Vec::with_capacity(N);
        let mut roundtrip = Vec::with_capacity(N);
        for i in 0..N + 1000 {
            let started = Instant::now();
            let reply = handle.submit_snapshot_lease_claim("owner".into(), Duration::from_secs(45)).unwrap();
            let submitted = started.elapsed().as_nanos();
            owner.execute(owner.receiver().try_recv().unwrap());
            let ticket = reply.try_recv().unwrap().unwrap().unwrap();
            let finished = started.elapsed().as_nanos();
            if i >= 1000 { enqueue.push(submitted); roundtrip.push(finished); }
            handle.submit_snapshot_lease_finish(ticket, None).unwrap();
            owner.execute(owner.receiver().try_recv().unwrap());
        }
        for (name, mut samples) in [("claim_enqueue", enqueue), ("claim_to_reply", roundtrip)] {
            samples.sort_unstable();
            let q = |p: usize| samples[(N * p).div_ceil(1000) - 1];
            eprintln!("snapshot_lease_message stage={name} n={N} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_high_water=1 overflow=0 fixture=single_thread_owner_dispatch excludes=network_and_cross_core_scheduling", q(500), q(990), q(999), q(1000));
        }
    }
}
