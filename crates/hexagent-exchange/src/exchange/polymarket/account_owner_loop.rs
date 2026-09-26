//! Unified private ingress / execution lifecycle owner. The cold owner creates
//! GC plans from immutable publications. Only this owner mutates execution and
//! fill history, in bounded chunks interleaved with live lifecycle messages.
use super::super::user_feed::unified_private_owner::PrivateIngress;
use super::*;

pub(super) struct PrivateIngressInstall {
    pub ingress: Box<dyn PrivateIngress>,
    pub ready: crossbeam_channel::Sender<Result<(), String>>,
}

pub(super) struct RuntimeRetirement {
    tokens: HashSet<String>,
    coids: std::vec::IntoIter<String>,
    history: Option<super::super::live_position::LiveHistoryRetirement>,
}
impl RuntimeRetirement {
    // Cold-thread scan of an immutable publication; no live owner borrow.
    pub(super) fn prepare(shared: &SharedState) -> Option<Self> {
        if !shared.account_state.has_settled_gc_candidates() {
            return None;
        }
        let tokens: HashSet<_> = shared
            .account_state
            .finalize_ready_settled_audit_retirements()
            .into_iter()
            .flatten()
            .collect();
        if tokens.is_empty() {
            return None;
        }
        let snapshot = shared.execution_state.load_full();
        let coids = snapshot
            .coid_to_token
            .iter()
            .filter(|(_, token)| tokens.contains(*token))
            .map(|(coid, _)| coid.clone())
            .collect::<Vec<_>>()
            .into_iter();
        Some(Self {
            tokens,
            coids,
            history: None,
        })
    }

    fn step(
        &mut self,
        shared: &SharedState,
        execution: &mut ExecutionStateOwner,
        positions: &mut LivePositionManager,
    ) -> bool {
        if self.history.is_none() {
            self.history = Some(positions.retirement(&self.tokens));
        }
        let mut retired = RetiredRuntimeMappings::default();
        for coid in self.coids.by_ref().take(16) {
            // Recheck owner state: a stale cold plan must never remove a
            // newly rebound identity belonging to a different market.
            if !execution
                .coid_to_token
                .get(&coid)
                .is_some_and(|token| self.tokens.contains(token))
            {
                continue;
            }
            if let Some(oid) = execution.coid_to_oid.remove(&coid) {
                let normalized = normalize_order_id(&oid);
                if execution.oid_to_coid.get(&normalized) == Some(&coid) {
                    execution.oid_to_coid.remove(&normalized);
                    shared
                        .runtime_order_ownership
                        .remove_client_order(&oid, &coid);
                    retired.normalized_order_ids.push(normalized);
                }
            }
            execution.coid_to_token.remove(&coid);
            retired.client_order_ids.push(coid);
        }
        if retired.len() > 0 {
            let mut next = (*shared.execution_state.load_full()).clone();
            retired.apply_to(&mut next);
            shared.execution_state.store(Arc::new(next));
            shared.enqueue_lifecycle_trace(LifecycleTraceJob::ForgetMany {
                client_order_ids: retired.client_order_ids.into_iter().collect(),
            });
        }
        let history = self.history.as_mut().unwrap();
        positions.prune_terminal_step(history, 16);
        self.coids.len() == 0 && history.is_empty()
    }
}

pub(super) fn run(
    weak: std::sync::Weak<SharedState>,
    shutdown: hexagent_runtime::shutdown::ShutdownToken,
    shutdown_rx: crossbeam_channel::Receiver<ShutdownPhase>,
    lifecycle_rx: crossbeam_channel::Receiver<AccountLifecycleJob>,
    account_owner: SharedAccountLifecycleOwnerState,
    maintenance_rx: crossbeam_channel::Receiver<AccountMaintenanceJob>,
    settled_gc_rx: crossbeam_channel::Receiver<()>,
    install_rx: crossbeam_channel::Receiver<PrivateIngressInstall>,
    gc_ready_rx: crossbeam_channel::Receiver<RuntimeRetirement>,
    unified: bool,
    recovery: Option<super::super::live_position::RecoveryOwnerBinding>,
    mut execution: ExecutionStateOwner,
    mut positions: LivePositionManager,
) {
    crate::latency::prepare_thread_stages(&[
        "polymarket.account.owner_command",
        "polymarket.account.owner_execution",
        "polymarket.account.owner_ingress",
        "polymarket.account.owner_maintenance",
        "polymarket.account.owner_retirement",
    ]);
    let mut replay = super::super::user_feed::PrivateReplayOwner::new();
    let mut ingress: Option<Box<dyn PrivateIngress>> = None;
    let mut retirement: Option<RuntimeRetirement> = None;
    let mut turns = 0usize;
    loop {
        let Some(shared) = weak.upgrade() else {
            break;
        };
        let started = crate::latency::Instant::now();
        let mut worked = recovery.as_ref().is_some_and(|owner| owner.service_one());
        if let Ok(install) = install_rx.try_recv() {
            if ingress.is_some() {
                let _ = install
                    .ready
                    .send(Err("private ingress already installed".into()));
            } else {
                install.ingress.install(&shared);
                ingress = Some(install.ingress);
                crate::latency::prepare_polymarket_private_stages();
                let _ = install.ready.send(Ok(()));
            }
            worked = true;
        }
        // One message from each lifecycle source per turn: a continuously
        // runnable producer cannot starve the other lifecycle lane or ingress.
        if let Ok(command) = account_owner.receiver().try_recv() {
            let stage_started = crate::latency::Instant::now();
            account_owner.execute(command);
            crate::latency::record("polymarket.account.owner_command", stage_started);
            worked = true;
        }
        if let Ok(job) = lifecycle_rx.try_recv() {
            let stage_started = crate::latency::Instant::now();
            shared.apply_account_lifecycle_job(&mut execution, &mut positions, &mut replay, job);
            crate::latency::record("polymarket.account.owner_execution", stage_started);
            worked = true;
        }
        if let Some(ingress) = ingress.as_mut() {
            let stage_started = crate::latency::Instant::now();
            if ingress.step(&shared, &mut positions, &mut replay) {
                crate::latency::record("polymarket.account.owner_ingress", stage_started);
                worked = true;
            }
        }
        turns = turns.wrapping_add(1);
        // Maintenance gets a bounded opportunity every 64 active turns; it
        // cannot hold up an entire private burst, nor starve indefinitely.
        if !worked || turns % 64 == 0 {
            if let Ok(job) = maintenance_rx.try_recv() {
                let stage_started = crate::latency::Instant::now();
                shared.apply_account_maintenance_job(job);
                crate::latency::record("polymarket.account.owner_maintenance", stage_started);
                worked = true;
            }
            if unified {
                if retirement.is_none() {
                    retirement = gc_ready_rx.try_recv().ok();
                }
                if let Some(work) = retirement.as_mut() {
                    let stage_started = crate::latency::Instant::now();
                    if work.step(&shared, &mut execution, &mut positions) {
                        retirement = None;
                    }
                    crate::latency::record("polymarket.account.owner_retirement", stage_started);
                    worked = true;
                }
            } else if settled_gc_rx.try_recv().is_ok() || !worked {
                shared.run_settled_gc_pass(&mut execution, &mut positions);
            }
        }
        if worked {
            crate::latency::record("polymarket.account.owner_turn", started);
            continue;
        }
        // Producers are stopped before Finished. A stopped downstream consumer
        // can prevent outbox drain: preserve the failed-closed health marker
        // and require authoritative replay on restart, never claim delivery.
        if shutdown.is_finished() {
            if shared.user_feed_health.private_delivery_pending() {
                shared.user_feed_health.set_inventory_uncertain(true);
                log::error!(
                    "private owner stopped with undelivered updates; authoritative replay required"
                );
            }
            break;
        }
        let wait_for_work = |recovery_rx: Option<
            &crossbeam_channel::Receiver<super::super::live_position::RecoveryDeliveryCommand>,
        >| {
            let mut wait = crossbeam_channel::Select::new();
            wait.recv(account_owner.receiver());
            wait.recv(&lifecycle_rx);
            wait.recv(&maintenance_rx);
            wait.recv(&install_rx);
            wait.recv(&shutdown_rx);
            if unified {
                wait.recv(&gc_ready_rx);
            } else {
                wait.recv(&settled_gc_rx);
            }
            if let Some(rx) = recovery_rx {
                wait.recv(rx);
            }
            if !shared.user_feed_health.private_delivery_pending() {
                if let Some(ingress) = ingress.as_ref() {
                    ingress.register(&mut wait);
                }
            }
            let delay = if shared.user_feed_health.private_delivery_pending() {
                hexagent_runtime::poll_channel::IDLE_POLL
            } else {
                Duration::from_millis(100)
            };
            let _ = wait.ready_timeout(delay);
        };
        if let Some(recovery) = recovery.as_ref() {
            recovery.with_receiver(|rx| wait_for_work(Some(rx)));
        } else {
            wait_for_work(None);
        }
        let _ = shutdown_rx.try_recv();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retirement_is_bounded_and_preserves_rebound_and_unrelated_identity() {
        let shared = super::super::tests::shutdown_test_trade(ShutdownToken::new()).shared_state();
        let mut initial = ExecutionStateSnapshot::default();
        for i in 0..40 {
            let coid = format!("retire-{i}");
            let oid = format!("0x{:064x}", i + 1);
            initial.coid_to_oid = initial.coid_to_oid.with_insert(coid.clone(), oid.clone());
            initial.oid_to_coid = initial
                .oid_to_coid
                .with_insert(normalize_order_id(&oid), coid.clone());
            initial.coid_to_token = initial.coid_to_token.with_insert(coid, "old".into());
        }
        let mut owner = ExecutionStateOwner::new(initial.clone());
        shared.execution_state.store(Arc::new(initial.clone()));
        // Simulate an owner-local identity rebind after the cold snapshot.
        owner.coid_to_token.insert("retire-0".into(), "new".into());
        let mut work = RuntimeRetirement {
            tokens: HashSet::from(["old".into()]),
            coids: (0..40)
                .map(|i| format!("retire-{i}"))
                .collect::<Vec<_>>()
                .into_iter(),
            history: None,
        };
        let mut positions = LivePositionManager::new();
        assert!(!work.step(&shared, &mut owner, &mut positions));
        assert!(
            owner.coid_to_oid.len() >= 24,
            "one turn removed more than 16 identities"
        );
        while !work.step(&shared, &mut owner, &mut positions) {}
        assert_eq!(owner.coid_to_oid.len(), 1);
        assert!(owner.coid_to_oid.contains_key("retire-0"));
        assert_eq!(
            initial.coid_to_oid.len(),
            40,
            "published readers are immutable"
        );
    }
}
