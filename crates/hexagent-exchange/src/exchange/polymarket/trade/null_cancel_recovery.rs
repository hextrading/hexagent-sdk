//! Per-instance cold recovery state; only run_poly_orphan_recovery mutates it.
//! Deadlines return control to the existing worker. No sleeping HTTP/quote lane,
//! new thread, shared mutable map, or authoritative exchange proof is introduced.
use super::*;

const CAPACITY: usize = 128;
pub const INFERRED_CANCEL: &str = "orphan_reconcile_two_null_cancelled";

struct Attempt {
    ownership: OrderOwnership,
    nulls: u8,
    due_ns: u64,
    last_slot: Option<usize>,
    placement: bool,
}

#[cfg(test)]
mod tests {
    use super::super::reconcile_identity_tests::{install, ownership};
    use super::*;
    use crate::http1_pool::Role;

    fn setup() -> (ShutdownToken, PolymarketTrade, OrderOwnership) {
        let shutdown = ShutdownToken::new();
        let mut trade = super::super::tests::shutdown_test_trade(shutdown.clone());
        trade.instance_id = "btc01".into();
        let order = ownership(Side::Sell, "btc01-null-policy", "0xnull-policy", "btc01");
        install(&trade.shared, &order);
        (shutdown, trade, order)
    }

    fn stop(shutdown: ShutdownToken, trade: PolymarketTrade) {
        shutdown.request();
        shutdown.finish();
        trade.shared.join_background_workers();
    }

    #[test]
    fn two_nulls_require_500ms_deadline_and_release_only_exact_owner() {
        let (shutdown, trade, order) = setup();
        let sibling = ownership(Side::Sell, "btc02-sibling", "0xsibling", "btc02");
        install(&trade.shared, &sibling);
        let (transport, requests) = super::super::super::rtt_probe::probe_http_lane(2);
        trade.shared.bind_recovery_http_transport(transport);
        let server = std::thread::spawn(move || {
            for slot in [0, 1] {
                let r = requests.recv_timeout(Duration::from_secs(2)).unwrap();
                assert_eq!(r.instance_id, "btc01");
                assert_eq!(r.excluded_slot, if slot == 0 { None } else { Some(0) });
                r.reply_for_test(Ok(serde_json::Value::Null), Some((Role::Reconcile, slot)));
            }
            assert!(requests.recv_timeout(Duration::from_millis(30)).is_err());
        });
        let mut policy = NullCancelRecovery::new();
        let started = now_ns();
        assert!(matches!(
            trade.null_cancel_step(
                &mut policy,
                &order.client_order_id,
                &order.order_id,
                false,
                started
            ),
            Step::Update(_)
        ));
        let due = policy.entries[0].due_ns;
        assert!(due >= started + 500_000_000);
        assert!(matches!(
            trade.null_cancel_step(
                &mut policy,
                &order.client_order_id,
                &order.order_id,
                false,
                due - 1
            ),
            Step::Update(_)
        ));
        assert_eq!(policy.entries[0].nulls, 1);
        let Step::Update(update) = trade.null_cancel_step(
            &mut policy,
            &order.client_order_id,
            &order.order_id,
            false,
            due,
        ) else {
            panic!("no terminal");
        };
        assert_eq!(update.status, OrderStatus::Cancelled);
        assert_eq!(update.error.as_deref(), Some(INFERRED_CANCEL));
        let cancelled = trade
            .shared
            .account_state
            .order(&order.client_order_id)
            .unwrap();
        assert!(cancelled.inferred_cancel);
        assert_eq!(cancelled.reserved_quantity, 0.0);
        assert!(!cancelled.terminal_trade_ids_authoritative);
        assert_eq!(
            trade
                .shared
                .account_state
                .order(&sibling.client_order_id)
                .unwrap()
                .reserved_quantity,
            16.0
        );
        assert!(policy.entries.is_empty());
        server.join().unwrap();
        stop(shutdown, trade);
    }

    #[test]
    fn filled_before_retry_and_during_null_response_suppresses_later_queries() {
        for during_request in [false, true] {
            let (shutdown, trade, order) = setup();
            let (transport, requests) = super::super::super::rtt_probe::probe_http_lane(2);
            trade.shared.bind_recovery_http_transport(transport);
            let shared = trade.shared.clone();
            let coid = order.client_order_id.clone();
            let server = std::thread::spawn(move || {
                let r = requests.recv_timeout(Duration::from_secs(2)).unwrap();
                if during_request {
                    shared
                        .account_state
                        .mark_order_status_effective(&coid, OrderStatus::Filled);
                }
                r.reply_for_test(Ok(serde_json::Value::Null), Some((Role::Reconcile, 0)));
                assert!(requests.recv_timeout(Duration::from_millis(50)).is_err());
            });
            let mut policy = NullCancelRecovery::new();
            let first = trade.null_cancel_step(
                &mut policy,
                &order.client_order_id,
                &order.order_id,
                false,
                now_ns(),
            );
            if during_request {
                assert!(matches!(
                    first,
                    Step::Update(OrderUpdate {
                        status: OrderStatus::Filled,
                        ..
                    })
                ));
            } else {
                trade
                    .shared
                    .account_state
                    .mark_order_status_effective(&order.client_order_id, OrderStatus::Filled);
                let due = policy.entries[0].due_ns;
                assert!(matches!(
                    trade.null_cancel_step(
                        &mut policy,
                        &order.client_order_id,
                        &order.order_id,
                        false,
                        due
                    ),
                    Step::Update(OrderUpdate {
                        status: OrderStatus::Filled,
                        ..
                    })
                ));
            }
            assert!(policy.entries.is_empty());
            assert!(
                !trade
                    .shared
                    .account_state
                    .order(&order.client_order_id)
                    .unwrap()
                    .inferred_cancel
            );
            server.join().unwrap();
            stop(shutdown, trade);
        }
    }

    #[test]
    fn error_breaks_null_streak_and_capacity_or_wrong_owner_cannot_cancel() {
        let (shutdown, trade, order) = setup();
        let (transport, requests) = super::super::super::rtt_probe::probe_http_lane(2);
        trade.shared.bind_recovery_http_transport(transport);
        let server = std::thread::spawn(move || {
            for reply in [
                Ok(serde_json::Value::Null),
                Err(HttpErr::Status(500, "unavailable".into())),
                Ok(serde_json::Value::Null),
            ] {
                requests
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap()
                    .reply_for_test(reply, Some((Role::Reconcile, 0)));
            }
        });
        let mut policy = NullCancelRecovery::new();
        trade.null_cancel_step(
            &mut policy,
            &order.client_order_id,
            &order.order_id,
            false,
            now_ns(),
        );
        let due = policy.entries[0].due_ns;
        assert!(matches!(
            trade.null_cancel_step(
                &mut policy,
                &order.client_order_id,
                &order.order_id,
                false,
                due
            ),
            Step::Fallback(Some(_))
        ));
        assert!(policy.entries.is_empty());
        trade.null_cancel_step(
            &mut policy,
            &order.client_order_id,
            &order.order_id,
            false,
            now_ns(),
        );
        assert_eq!(policy.entries[0].nulls, 1);
        assert_eq!(
            trade
                .shared
                .account_state
                .order(&order.client_order_id)
                .unwrap()
                .reserved_quantity,
            16.0
        );
        policy.remove(&order.client_order_id);
        for n in 0..CAPACITY {
            let mut o = order.clone();
            o.client_order_id = format!("full-{n}");
            policy.entries.push(Attempt {
                ownership: o,
                nulls: 1,
                due_ns: u64::MAX,
                last_slot: Some(0),
                placement: false,
            });
        }
        assert!(matches!(
            trade.null_cancel_step(
                &mut policy,
                &order.client_order_id,
                &order.order_id,
                false,
                now_ns()
            ),
            Step::Fallback(None)
        ));
        let other = ownership(Side::Sell, "btc02-other", "0xother", "btc02");
        install(&trade.shared, &other);
        assert!(matches!(
            trade.null_cancel_step(
                &mut policy,
                &other.client_order_id,
                &other.order_id,
                false,
                now_ns()
            ),
            Step::Fallback(None)
        ));
        server.join().unwrap();
        stop(shutdown, trade);
    }

    #[test]
    fn unknown_placement_requires_ambiguous_delete_then_two_null_queries() {
        let (shutdown, trade, order) = setup();
        trade
            .shared
            .account_state
            .mark_order_status_effective(&order.client_order_id, OrderStatus::NewOrderTimeout);
        let (transport, requests) = super::super::super::rtt_probe::probe_http_lane(2);
        trade.shared.bind_recovery_http_transport(transport);
        let oid = order.order_id.clone();
        let server = std::thread::spawn(move || {
            let cancel = requests.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(cancel.role, Role::Cancel);
            cancel.reply_for_test(Ok(serde_json::json!({"canceled":[],"not_canceled":{oid:"order can't be found - already canceled or matched"}})), Some((Role::Cancel, 0)));
            for slot in [0, 1] {
                let get = requests.recv_timeout(Duration::from_secs(2)).unwrap();
                assert_eq!(get.role, Role::Reconcile);
                get.reply_for_test(Ok(serde_json::Value::Null), Some((Role::Reconcile, slot)));
            }
        });
        let mut policy = NullCancelRecovery::new();
        let Step::Update(first) = trade.null_cancel_step(
            &mut policy,
            &order.client_order_id,
            &order.order_id,
            true,
            now_ns(),
        ) else {
            panic!("missing backoff");
        };
        assert_eq!(first.status, OrderStatus::NewOrderTimeout);
        let due = policy.entries[0].due_ns;
        let Step::Update(last) = trade.null_cancel_step(
            &mut policy,
            &order.client_order_id,
            &order.order_id,
            true,
            due,
        ) else {
            panic!("missing cancellation");
        };
        assert_eq!(last.status, OrderStatus::Cancelled);
        assert_eq!(last.error.as_deref(), Some(INFERRED_CANCEL));
        server.join().unwrap();
        stop(shutdown, trade);
    }
}

pub struct NullCancelRecovery {
    entries: Vec<Attempt>,
}

impl Default for NullCancelRecovery {
    fn default() -> Self {
        Self::new()
    }
}

impl NullCancelRecovery {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(CAPACITY),
        }
    }
    /// Cold coordinator's timer; independent of quote/watchdog cadence.
    pub fn next_deadline_ns(&self) -> Option<u64> {
        self.entries.iter().map(|e| e.due_ns).min()
    }

    pub fn due_orders(
        &self,
        now: u64,
    ) -> (
        Vec<(String, String, Side, f64, Option<String>)>,
        Vec<(String, String)>,
    ) {
        let mut places = Vec::new();
        let mut cancels = Vec::new();
        for entry in self.entries.iter().filter(|e| e.due_ns <= now) {
            let o = &entry.ownership;
            if entry.placement {
                places.push((
                    o.client_order_id.clone(),
                    o.token_id.clone(),
                    o.side,
                    o.price,
                    Some(o.order_id.clone()),
                ));
            } else {
                cancels.push((o.client_order_id.clone(), o.order_id.clone()));
            }
        }
        (places, cancels)
    }

    fn index(&self, coid: &str) -> Option<usize> {
        self.entries
            .iter()
            .position(|e| e.ownership.client_order_id == coid)
    }
    fn remove(&mut self, coid: &str) {
        if let Some(i) = self.index(coid) {
            self.entries.swap_remove(i);
        }
    }
}

enum Step {
    Fallback(Option<FetchOrderResult>),
    Update(OrderUpdate),
}

impl PolymarketTrade {
    /// Existing pinned per-instance coordinator owns `policy`. Overflow leaves
    /// the order under ordinary recovery and retains risk; entries are swept
    /// when private events have already terminalized their exact owner.
    pub fn reconcile_orphans_with_null_backoff(
        &self,
        policy: &mut NullCancelRecovery,
        places: &[(String, String, Side, f64, Option<String>)],
        cancels: &[(String, String)],
        trades: &[String],
    ) -> Vec<OrderUpdate> {
        policy.entries.retain(|e| {
            self.shared
                .account_state
                .order(&e.ownership.client_order_id)
                .is_some_and(|o| {
                    !matches!(o.status, OrderStatus::Filled | OrderStatus::Rejected)
                        && !(o.status == OrderStatus::Cancelled
                            && o.reserved_cash == 0.0
                            && o.reserved_quantity == 0.0)
                })
        });
        let mut updates = Vec::new();
        for place in places {
            let Some(oid) = place.4.as_deref() else {
                continue;
            };
            match self.null_cancel_step(policy, &place.0, oid, true, now_ns()) {
                Step::Update(update) => updates.push(update),
                Step::Fallback(prefetched) => updates.extend(self.reconcile_orphans_prefetched(
                    None,
                    true,
                    std::slice::from_ref(place),
                    &[],
                    &[],
                    prefetched,
                )),
            }
        }
        for cancel in cancels {
            match self.null_cancel_step(policy, &cancel.0, &cancel.1, false, now_ns()) {
                Step::Update(update) => updates.push(update),
                Step::Fallback(prefetched) => updates.extend(self.reconcile_orphans_prefetched(
                    None,
                    true,
                    &[],
                    std::slice::from_ref(cancel),
                    &[],
                    prefetched,
                )),
            }
        }
        if !trades.is_empty() {
            updates.extend(self.reconcile_orphans_via_owners(&[], &[], trades));
        }
        updates
    }

    fn null_policy_update(
        &self,
        order: &OrderOwnership,
        status: OrderStatus,
        error: String,
    ) -> OrderUpdate {
        OrderUpdate {
            order_slot: order.order_slot,
            client_order_id: order.client_order_id.clone(),
            exchange: Exchange::Polymarket,
            symbol: order.token_id.clone(),
            side: order.side,
            exchange_order_id: Some(order.order_id.clone()),
            status,
            liquidity: None,
            filled_quantity: 0.0,
            remaining_quantity: 0.0,
            avg_fill_price: order.price,
            timestamp_ns: now_ns(),
            exchange_event_timestamp_ns: None,
            trade_id: None,
            trade_fee: None,
            order_audit: None,
            error: Some(error),
        }
    }

    fn null_policy_terminal(
        &self,
        policy: &mut NullCancelRecovery,
        order: &OrderOwnership,
    ) -> Option<Step> {
        if order.status == OrderStatus::Filled
            || (order.status == OrderStatus::Cancelled
                && order.reserved_cash == 0.0
                && order.reserved_quantity == 0.0)
        {
            policy.remove(&order.client_order_id);
            log::info!("[null_cancel_policy] coid={} result=already_terminal status={:?} further_queries=false",
                order.client_order_id, order.status);
            return Some(Step::Update(
                self.null_policy_update(
                    order,
                    order.status,
                    if order.inferred_cancel {
                        INFERRED_CANCEL
                    } else {
                        ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL
                    }
                    .into(),
                ),
            ));
        }
        None
    }

    fn null_cancel_step(
        &self,
        policy: &mut NullCancelRecovery,
        coid: &str,
        oid: &str,
        placement: bool,
        now: u64,
    ) -> Step {
        let Some(order) = self
            .shared
            .account_state
            .recovery_order(coid)
            .ok()
            .flatten()
        else {
            policy.remove(coid);
            return Step::Fallback(None);
        };
        if order.instance_id != self.instance_id || order.order_id != oid || oid.is_empty() {
            policy.remove(coid);
            return Step::Fallback(None);
        }
        if let Some(done) = self.null_policy_terminal(policy, &order) {
            return done;
        }
        let Some(transport) = self.shared.recovery_http.get() else {
            policy.remove(coid);
            return Step::Fallback(None);
        };
        if let Some(i) = policy.index(coid) {
            let old = &policy.entries[i].ownership;
            if old.order_id != oid
                || old.order_slot != order.order_slot
                || old.instance_id != order.instance_id
            {
                policy.remove(coid);
                return Step::Fallback(None);
            }
        } else {
            if policy.entries.len() == CAPACITY {
                log::warn!("[null_cancel_policy] coid={} result=capacity_exhausted capacity={} reservation_retained=true", coid, CAPACITY);
                return Step::Fallback(None);
            }
            if placement {
                // An unknown POST alone is not eligible. First obtain an actual
                // ambiguous exact-order DELETE reply; never submit POST again.
                let body = serde_json::json!({"orderID": oid}).to_string();
                let reply = transport.request(
                    &self.shared,
                    &self.instance_id,
                    "DELETE",
                    "/order",
                    &body,
                    None,
                    None,
                );
                if !matches!(reply.location, Some((crate::http1_pool::Role::Cancel, _))) {
                    return Step::Fallback(None);
                }
                let Ok(json) = reply.reply else {
                    return Step::Fallback(None);
                };
                if exact_cancel_acknowledged(&json, oid) {
                    if let Ok(confirmed) = self
                        .shared
                        .account_state
                        .confirm_recovery_cancellation(&order)
                    {
                        if let Some(update) = self.cancel_order_via_owners(&confirmed, oid) {
                            return Step::Update(update);
                        }
                    }
                    return Step::Fallback(None);
                }
                if cancel_delete_response_outcome(&json, oid) == CancelReasonOutcome::Filled {
                    if let Ok(identity) = self.shared.reconcile_order_identity(coid, oid) {
                        let mut completed = Vec::new();
                        self.finish_reconciled_cancel(
                            None,
                            true,
                            coid,
                            oid,
                            identity,
                            OrderStatus::Filled,
                            "FILLED",
                            None,
                            None,
                            &mut completed,
                        );
                        if let Some(update) = completed.pop() {
                            return Step::Update(update);
                        }
                    }
                    return Step::Fallback(None);
                }
                if cancel_delete_response_outcome(&json, oid) != CancelReasonOutcome::Uncertain
                    || json
                        .get("not_canceled")
                        .and_then(|v| v.get(oid))
                        .and_then(|v| v.as_str())
                        .is_none()
                {
                    return Step::Fallback(None);
                }
            }
            policy.entries.push(Attempt {
                ownership: order.clone(),
                nulls: 0,
                due_ns: now,
                last_slot: None,
                placement,
            });
        }
        let i = policy.index(coid).expect("inserted above");
        if now < policy.entries[i].due_ns {
            let delay_ms = (policy.entries[i].due_ns - now).div_ceil(1_000_000);
            return Step::Update(self.null_policy_update(
                &order,
                if placement {
                    OrderStatus::NewOrderTimeout
                } else {
                    OrderStatus::CancelUncertain
                },
                format!(
                    "{}{};evidence=null_backoff",
                    ORPHAN_RECONCILE_RETRY_AFTER_MS_PREFIX, delay_ms
                ),
            ));
        }
        let path = format!("/data/order/{oid}");
        let lookup_started_ns = now_ns();
        log::info!("[null_cancel_policy] coid={} orderID={} stage=lookup_started query={} started_ns={} tracked_orders={} capacity={}",
            coid, oid, policy.entries[i].nulls + 1, lookup_started_ns, policy.entries.len(), CAPACITY);
        let reply = transport.request(
            &self.shared,
            &self.instance_id,
            "GET",
            &path,
            "",
            None,
            policy.entries[i].last_slot,
        );
        let location = reply.location;
        let fetched = self.classify_order_lookup_reply(coid, oid, reply.reply);
        log::info!(
            "[null_cancel_policy] coid={} stage=lookup_complete result={} elapsed_us={}",
            coid,
            fetch_order_result_name(&fetched),
            now_ns().saturating_sub(lookup_started_ns) / 1000
        );
        // A Filled private push racing any HTTP result wins before scheduling
        // another deadline, and the final commit repeats this check on owner.
        if let Some(current) = self
            .shared
            .account_state
            .recovery_order(coid)
            .ok()
            .flatten()
        {
            if current.order_id != oid
                || current.order_slot != order.order_slot
                || current.instance_id != self.instance_id
            {
                policy.remove(coid);
                return Step::Fallback(None);
            }
            if let Some(done) = self.null_policy_terminal(policy, &current) {
                return done;
            }
        }
        let literal_null = matches!(&fetched, FetchOrderResult::Unavailable(FetchUnavailable::InvalidResponse(body)) if body == "null");
        if !literal_null
            || policy.entries[i]
                .last_slot
                .is_some_and(|excluded| location.is_some_and(|(_, slot)| slot == excluded))
            || !matches!(location, Some((crate::http1_pool::Role::Reconcile, _)))
        {
            policy.remove(coid);
            return Step::Fallback(Some(fetched));
        }
        let entry = &mut policy.entries[i];
        entry.nulls += 1;
        entry.last_slot = location.map(|(_, slot)| slot);
        let delay_ms = if entry.nulls == 1 { 500 } else { 0 };
        log::info!(
            "[null_cancel_policy] coid={} orderID={} null_count={} retry_ms={} location={:?}",
            coid,
            oid,
            entry.nulls,
            delay_ms,
            location
        );
        if delay_ms != 0 {
            entry.due_ns = now_ns().saturating_add(delay_ms * 1_000_000);
            return Step::Update(self.null_policy_update(
                &order,
                if placement {
                    OrderStatus::NewOrderTimeout
                } else {
                    OrderStatus::CancelUncertain
                },
                format!(
                    "{}{};evidence=literal_json_null",
                    ORPHAN_RECONCILE_RETRY_AFTER_MS_PREFIX, delay_ms
                ),
            ));
        }
        policy.remove(coid);
        match self.shared.account_state.infer_null_cancellation(&order) {
            Ok(committed) => {
                if committed.inferred_cancel {
                    self.shared
                        .remove_order_after_authoritative_commit(coid, committed.status);
                }
                log::warn!("[null_cancel_policy] coid={} result=policy_complete status={:?} inferred={} late_fills_retained=true",
                    coid, committed.status, committed.inferred_cancel);
                Step::Update(
                    self.null_policy_update(
                        &committed,
                        committed.status,
                        if committed.inferred_cancel {
                            INFERRED_CANCEL
                        } else {
                            ORPHAN_RECONCILE_AUTHORITATIVE_TERMINAL
                        }
                        .into(),
                    ),
                )
            }
            Err(error) => {
                log::warn!(
                    "[null_cancel_policy] coid={} result=owner_rejected reason={}",
                    coid,
                    error
                );
                Step::Fallback(Some(fetched))
            }
        }
    }
}
