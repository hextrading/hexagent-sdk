//! Price-only live CLOB state. The existing CLOB owner is the sole writer;
//! subscription setup owns the keys, and steady-state updates touch scalars.
//! QuoteTick's owned symbol is the existing cross-thread event boundary. No
//! depth, quantities, pending consistency checks, repair requests or deadlines
//! are created here. Paper/record continue through the full-depth implementation.
use super::*;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct LiveBbo {
    bid: f64,
    ask: f64,
    pub(super) exchange_timestamp_ns: u64,
    // Transport provenance for prewarmed subscription handoff, never ordering.
    local_timestamp_ns: u64,
}

fn wire_price(value: &WireDecimal<'_>) -> Option<f64> {
    match value {
        WireDecimal::String(value) => value.parse().ok(),
        WireDecimal::U64(value) => Some(*value as f64),
        WireDecimal::I64(value) => Some(*value as f64),
        WireDecimal::F64(value) => Some(*value),
    }
}

fn in_range(price: f64) -> bool {
    price.is_finite() && (0.0..=1.0).contains(&price)
}

fn record_source_age(stage: &'static str, server_ns: u64, receive_ns: u64) {
    // Different wall clocks: an indicative source age, never pure network RTT.
    // Missing/future timestamps must not become zero-duration observations.
    if server_ns != 0 {
        if let Some(age) = receive_ns.checked_sub(server_ns) {
            crate::latency::observe_ns(stage, age);
        }
    }
}

impl ClobLocalBooks {
    /// Candidate seeding runs while the active lane keeps receiving quotes.
    /// Do not roll a common condition's cursor back to the candidate's older
    /// snapshot at handoff. This owner-local boundary copy preserves both
    /// clocks; later out-of-order wire updates remain rejected by the cursor.
    pub(super) fn inherit_newer_live_bbo(&mut self, previous: &Self) -> usize {
        let (Some(next), Some(previous_live)) = (self.live_bbo.as_mut(), previous.live_bbo.as_ref()) else {
            return 0;
        };
        let mut retained = 0;
        for (condition, current) in next {
            let Some(old) = previous_live.get(condition) else { continue };
            if (old.exchange_timestamp_ns, old.local_timestamp_ns)
                > (current.exchange_timestamp_ns, current.local_timestamp_ns)
            {
                *current = *old;
                if let Some(health) = self.health_states.get_mut(condition) {
                    *health = if old.bid > 0.0 && old.ask < 1.0 {
                        MarketDataHealthState::Healthy
                    } else {
                        MarketDataHealthState::Degraded
                    };
                }
                retained += 1;
            }
        }
        retained
    }

    pub(super) fn for_subscription(subscription: &ClobSubscription) -> Self {
        let mut state =
            Self::new_with_depth(&subscription.canonical_events, !subscription.live_bbo_only);
        if subscription.live_bbo_only {
            let mut live = HashMap::with_capacity(subscription.canonical_events.len());
            for spec in &subscription.canonical_events {
                live.insert(spec.condition_id.clone(), LiveBbo::default());
                state
                    .health_states
                    .insert(spec.condition_id.clone(), MarketDataHealthState::Degraded);
            }
            state.live_bbo = Some(live);
        }
        state
    }

    fn publish_live_bbo(
        &mut self,
        token: &str,
        bid: Option<f64>,
        ask: Option<f64>,
        server_ns: u64,
        receive_ns: u64,
        batch: &mut ClobParsedBatch,
    ) {
        // Missing fields are malformed, unlike explicit 0/1 empty-side values.
        let (Some(mut bid), Some(mut ask)) = (bid, ask) else {
            batch.wire.ignored += 1;
            return;
        };
        if !in_range(bid)
            || !in_range(ask)
            || server_ns == 0
            || receive_ns == 0
            || server_ns > receive_ns.saturating_add(MAX_PUBLIC_EVENT_FUTURE_SKEW_NS)
        {
            batch.wire.ignored += 1;
            return;
        }
        let Some(role) = self.roles.get(token) else {
            batch.wire.ignored += 1;
            return;
        };
        // Both venue boundary values denote an absent tradeable side. Publish
        // explicit sentinels (bid=0, ask=1) so the strategy clears old prices.
        if bid == 0.0 || bid == 1.0 {
            bid = 0.0;
        }
        if ask == 0.0 || ask == 1.0 {
            ask = 1.0;
        }
        if bid >= ask {
            batch.wire.ignored += 1;
            return;
        }
        if role.is_down {
            (bid, ask) = (1.0 - ask, 1.0 - bid);
        }
        let current = self
            .live_bbo
            .as_mut()
            .unwrap()
            .get_mut(&role.condition_id)
            .unwrap();
        if server_ns < current.exchange_timestamp_ns {
            batch.wire.ignored += 1;
            return;
        }
        let first = current.exchange_timestamp_ns == 0;
        // One cursor across Up/Down and every wire message kind. Equal venue
        // milliseconds remain ordered by receipt, including quantity-only deltas.
        *current = LiveBbo {
            bid,
            ask,
            exchange_timestamp_ns: server_ns,
            local_timestamp_ns: receive_ns,
        };
        let (bid, ask) = (current.bid, current.ask);
        let state = if bid > 0.0 && ask < 1.0 {
            MarketDataHealthState::Healthy
        } else {
            MarketDataHealthState::Degraded
        };
        let changed_health = first || self.health_states.get(&role.condition_id) != Some(&state);
        if changed_health {
            *self.health_states.get_mut(&role.condition_id).unwrap() = state;
            batch.events.push(self.health_event(
                &role.condition_id,
                token,
                state,
                "live direct BBO checkpoint".to_owned(),
                receive_ns,
            ));
        }
        // Coalesce only within the already-received frame. Reuse the owned
        // symbol if a sibling Up/Down entry has already produced this quote.
        let previous = batch.events.iter().position(
            |event| matches!(event, MarketEvent::Quote(quote) if quote.symbol == role.up_token),
        );
        let symbol = if let Some(index) = previous {
            let MarketEvent::Quote(quote) = batch.events.remove(index) else {
                unreachable!()
            };
            quote.symbol
        } else {
            role.up_token.clone()
        };
        batch.events.push(MarketEvent::Quote(QuoteTick {
            exchange: Exchange::Polymarket,
            symbol,
            bid_price: bid,
            ask_price: ask,
            bid_qty: 0.0,
            ask_qty: 0.0,
            exchange_timestamp_ns: server_ns,
            local_timestamp_ns: receive_ns,
        }));
    }

    pub(super) fn apply_live_price_change(
        &mut self,
        fields: &PriceChangeFields<'_>,
        receive_ns: u64,
        batch: &mut ClobParsedBatch,
    ) {
        let started = Instant::now();
        let server_ns = normalized_timestamp_ns(fields.timestamp.as_ref()).unwrap_or(0);
        record_source_age(
            "polymarket.ws.clob_price_change_source_to_receive",
            server_ns,
            receive_ns,
        );
        batch.wire.price_change_entries += fields.price_changes.len() as u64;
        for change in &fields.price_changes {
            self.publish_live_bbo(
                &change.asset_id,
                change.best_bid.as_ref().and_then(wire_price),
                change.best_ask.as_ref().and_then(wire_price),
                server_ns,
                receive_ns,
                batch,
            );
        }
        observe_clob_hold("polymarket.ws.clob_live_bbo_apply", Instant::now(), started);
    }

    pub(super) fn apply_live_best_bid_ask(
        &mut self,
        fields: &BestBidAskFields<'_>,
        receive_ns: u64,
        batch: &mut ClobParsedBatch,
    ) {
        let started = Instant::now();
        let server_ns = normalized_timestamp_ns(fields.timestamp.as_ref()).unwrap_or(0);
        record_source_age(
            "polymarket.ws.clob_best_bid_ask_source_to_receive",
            server_ns,
            receive_ns,
        );
        self.publish_live_bbo(
            &fields.asset_id,
            fields.best_bid,
            fields.best_ask,
            server_ns,
            receive_ns,
            batch,
        );
        observe_clob_hold("polymarket.ws.clob_live_bbo_apply", Instant::now(), started);
    }

    pub(super) fn apply_live_book(
        &mut self,
        fields: &BookFields<'_>,
        receive_ns: u64,
        batch: &mut ClobParsedBatch,
    ) {
        // The initial snapshot is also an immediate price checkpoint. Scan it
        // once; never retain depth or derive future BBO from delta quantities.
        let started = Instant::now();
        fn top(levels: &[BookLevel<'_>], bid: bool) -> Option<f64> {
            let mut best = if bid { 0.0_f64 } else { 1.0_f64 };
            for level in levels {
                let price: f64 = level.price.parse().ok()?;
                let size: f64 = level.size.parse().ok()?;
                if !in_range(price) || !size.is_finite() || size < 0.0 {
                    return None;
                }
                if size == 0.0 || price == 0.0 || price == 1.0 {
                    continue;
                }
                best = if bid {
                    best.max(price)
                } else {
                    best.min(price)
                };
            }
            Some(best)
        }
        self.publish_live_bbo(
            &fields.asset_id,
            top(&fields.bids, true),
            top(&fields.asks, false),
            normalized_timestamp_ns(fields.timestamp.as_ref()).unwrap_or(0),
            receive_ns,
            batch,
        );
        observe_clob_hold("polymarket.ws.clob_live_bbo_apply", Instant::now(), started);
    }

    /// Subscription-boundary handoff, not steady-state processing. A quiet
    /// prewarmed event must not wait for its next wire update to expose prices.
    pub(super) fn live_checkpoints(&self, tokens: &[String]) -> Vec<MarketEvent> {
        let Some(live) = &self.live_bbo else {
            return Vec::new();
        };
        let mut events = Vec::with_capacity(tokens.len());
        for token in tokens {
            let Some(role) = self.roles.get(token) else {
                continue;
            };
            if role.is_down {
                continue;
            }
            let Some(bbo) = live.get(&role.condition_id) else {
                continue;
            };
            if bbo.exchange_timestamp_ns == 0 {
                continue;
            }
            events.push(MarketEvent::Quote(QuoteTick {
                exchange: Exchange::Polymarket,
                symbol: role.up_token.clone(),
                bid_price: bbo.bid,
                ask_price: bbo.ask,
                bid_qty: 0.0,
                ask_qty: 0.0,
                exchange_timestamp_ns: bbo.exchange_timestamp_ns,
                local_timestamp_ns: bbo.local_timestamp_ns,
            }));
            events.push(self.health_event(
                &role.condition_id,
                token,
                self.health_states[&role.condition_id],
                "live BBO subscription checkpoint".into(),
                bbo.local_timestamp_ns,
            ));
        }
        events
    }
}
