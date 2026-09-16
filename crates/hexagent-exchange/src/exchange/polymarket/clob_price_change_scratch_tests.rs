use super::*;

// Exact pre-change production function from SDK acf28b5399ffa12308220e74ba42b87408688ed0.
// Test-only behavioral and allocation/latency baseline; no mirrored new implementation.
impl ClobLocalBooks {
    fn apply_price_change_before_scratch(
        &mut self,
        fields: PriceChangeFields<'_>,
        received_at: Instant,
        local_now: u64,
        counters: &mut ClobWireCounters,
        diagnostics: &mut Vec<ClobDiagnostic>,
        active_tokens: &[String],
    ) -> (Vec<MarketEvent>, usize, Vec<String>) {
        let exchange_timestamp_ns = timestamp_value_to_ns(fields.timestamp.as_ref(), local_now);
        if self.try_apply_quantity_only(&fields, received_at, exchange_timestamp_ns, counters) {
            return (Vec::new(), 0, Vec::new());
        }
        let mut immediate = Vec::new();
        let entry_counts: HashMap<String, usize> =
            fields
                .price_changes
                .iter()
                .fold(HashMap::new(), |mut counts, change| {
                    *counts.entry(change.asset_id.to_string()).or_insert(0) += 1;
                    counts
                });
        let mut before: HashMap<String, (Option<Decimal>, Option<Decimal>)> = HashMap::new();
        let mut reported_bbo: HashMap<String, ReportedBbo> = HashMap::new();
        let mut off_tick_tokens: HashSet<String> = HashSet::new();

        for change in fields.price_changes {
            counters.price_change_entries = counters.price_change_entries.saturating_add(1);
            let token = change.asset_id;
            let emit_diagnostic = subscribed_token(active_tokens, &token);
            let Some(price) = change.price.decimal() else {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change",
                        detail: format!("token={token} reason=invalid_price"),
                    });
                }
                continue;
            };
            let Some(size) = change.size.decimal() else {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change",
                        detail: format!("token={token} reason=invalid_size"),
                    });
                }
                continue;
            };
            if price <= Decimal::ZERO || price >= Decimal::ONE || size < Decimal::ZERO {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change",
                        detail: format!("token={token} price={price} size={size}"),
                    });
                }
                continue;
            }
            if !self.price_is_on_current_tick(token.as_ref(), price) {
                off_tick_tokens.insert(token.to_string());
            }
            let Some(current_book) = self.token_books.get(token.as_ref()) else {
                counters.unseeded_deltas = counters.unseeded_deltas.saturating_add(1);
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "unseeded_price_change",
                        detail: format!("token={token} ts={exchange_timestamp_ns}"),
                    });
                }
                continue;
            };
            if exchange_timestamp_ns < current_book.exchange_timestamp_ns {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "stale_price_change",
                        detail: format!(
                            "token={token} incoming_ts={} current_ts={}",
                            exchange_timestamp_ns, current_book.exchange_timestamp_ns,
                        ),
                    });
                }
                continue;
            }
            let sequence = self.next_sequence();
            let book = self
                .token_books
                .get_mut(token.as_ref())
                .expect("book existence checked above");
            if !before.contains_key(token.as_ref()) {
                before.insert(token.to_string(), book.top());
            }
            let side = change.side.trim();
            let levels = if side.eq_ignore_ascii_case("BUY") {
                &mut book.bids
            } else if side.eq_ignore_ascii_case("SELL") {
                &mut book.asks
            } else {
                counters.ignored = counters.ignored.saturating_add(1);
                if emit_diagnostic {
                    diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change",
                        detail: format!("token={token} reason=unknown_side side={side}"),
                    });
                }
                continue;
            };
            if size == Decimal::ZERO {
                levels.remove(&price);
                counters.level_deletes = counters.level_deletes.saturating_add(1);
            } else {
                levels.insert(price, size);
                counters.level_upserts = counters.level_upserts.saturating_add(1);
            }
            book.exchange_timestamp_ns = exchange_timestamp_ns;
            // Assign sequence per entry, not per token after the frame. This
            // preserves the server's original price_changes[] order even when
            // Up and Down entries for one event are interleaved.
            book.wire_sequence = sequence;
            book.dirty_since.get_or_insert(received_at);
            let _ = change.hash;

            let reported = reported_bbo.entry(token.to_string()).or_default();
            if let Some(value) = change.best_bid.as_ref() {
                match value.decimal() {
                    Some(price) => reported.bid = Some(normalize_reported_bbo(price)),
                    None if emit_diagnostic => diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change_bbo",
                        detail: format!("token={token} side=bid"),
                    }),
                    None => {}
                }
            }
            if let Some(value) = change.best_ask.as_ref() {
                match value.decimal() {
                    Some(price) => reported.ask = Some(normalize_reported_bbo(price)),
                    None if emit_diagnostic => diagnostics.push(ClobDiagnostic {
                        key: "invalid_price_change_bbo",
                        detail: format!("token={token} side=ask"),
                    }),
                    None => {}
                }
            }
        }

        // The venue's advertised BBO describes a logical microbatch, but that
        // batch can span multiple WebSocket frames with the same millisecond
        // timestamp. Merge expectations by token+timestamp and publish only
        // after the local top agrees (or the short quiet window expires).
        let mut validation_tokens: HashSet<String> = reported_bbo.keys().cloned().collect();
        validation_tokens.extend(off_tick_tokens.iter().cloned());
        let mut validation_tokens: Vec<_> = validation_tokens.into_iter().collect();
        validation_tokens.sort();
        for token in validation_tokens {
            if !before.contains_key(&token) {
                continue;
            }
            let newer_expected = reported_bbo.remove(&token).unwrap_or_default();
            let actual = self
                .token_books
                .get(&token)
                .map(ClobLocalBook::top)
                .unwrap_or_default();
            let off_tick = off_tick_tokens.contains(&token);
            let summary = subscribed_token(active_tokens, &token).then(|| BboFrameSample {
                exchange_timestamp_ns,
                entries: entry_counts.get(&token).copied().unwrap_or(0),
                expected: newer_expected,
                actual,
            });
            let pending =
                self.pending_bbo
                    .entry(token.clone())
                    .or_insert_with(|| PendingBboCheck {
                        exchange_timestamp_ns,
                        expected: ReportedBbo::default(),
                        first_observed_at: received_at,
                        last_update_at: received_at,
                        saw_mismatch: false,
                        saw_newer_checkpoint: false,
                        frame_summaries: BboFrameHistory::default(),
                        awaiting_tick_change: false,
                    });
            if exchange_timestamp_ns > pending.exchange_timestamp_ns {
                // A newer advertised checkpoint supersedes the unfinished
                // older one. Apply the newer delta first, then validate the
                // latest state; never fail an old checkpoint at this boundary.
                pending.exchange_timestamp_ns = exchange_timestamp_ns;
                pending.expected = newer_expected;
                pending.last_update_at = received_at;
                pending.saw_newer_checkpoint = true;
                pending.awaiting_tick_change = off_tick;
                pending.saw_mismatch |= !pending.expected.matches(actual);
            } else if pending.exchange_timestamp_ns == exchange_timestamp_ns {
                pending.expected.merge(newer_expected);
                pending.last_update_at = received_at;
                pending.awaiting_tick_change |= off_tick;
                pending.saw_mismatch |= !pending.expected.matches(actual);
            }
            if let Some(summary) = summary {
                pending.frame_summaries.push(summary);
            }
            let advertised_l1 = if !pending.expected.matches(actual) {
                match (
                    pending.expected.bid.flatten(),
                    pending.expected.ask.flatten(),
                ) {
                    (Some(bid), Some(ask)) if bid < ask => Some((bid, ask)),
                    _ => None,
                }
            } else {
                None
            };
            if self.roles.contains_key(&token) {
                if let Some((bid, ask)) = advertised_l1 {
                    if let (Some(bid_price), Some(ask_price)) = (bid.to_f64(), ask.to_f64()) {
                        let quote = QuoteTick {
                            exchange: Exchange::Polymarket,
                            symbol: token.clone(),
                            bid_price,
                            bid_qty: 0.0,
                            ask_price,
                            ask_qty: 0.0,
                            exchange_timestamp_ns,
                            local_timestamp_ns: local_now,
                        };
                        if let Some(event) = self.canonicalize_quote(quote, received_at) {
                            immediate.push(event);
                        }
                    }
                }
            }
        }

        let mut touched_order: Vec<_> = before
            .keys()
            .filter_map(|token| {
                self.token_books
                    .get(token)
                    .map(|book| (book.wire_sequence, token.clone()))
            })
            .collect();
        touched_order.sort_by_key(|(sequence, _)| *sequence);
        let health_tokens: Vec<String> = touched_order
            .iter()
            .map(|(_, token)| token.clone())
            .collect();
        for (_, token) in touched_order {
            let (top_changed, semantically_valid) = self
                .token_books
                .get(&token)
                .map(|book| {
                    (
                        before.get(&token).copied() != Some(book.top()),
                        book.is_semantically_valid(),
                    )
                })
                .unwrap_or((false, false));
            let _ = self.resolve_pending_if_ready(&token, received_at, counters);
            if top_changed
                && semantically_valid
                && !self.pending_bbo.contains_key(&token)
                && !self.market_is_quarantined(&token)
            {
                if let Some(book) = self.token_books.get_mut(&token) {
                    book.dirty_since = None;
                }
                if let Some(event) = self.canonicalize_token(&token, local_now) {
                    push_latest_order_book(&mut immediate, event);
                }
            }
        }
        let mut reconciled_markets = HashSet::new();
        for token in health_tokens {
            if reconciled_markets.insert(self.market_key(&token)) {
                if let Some(event) = self.reconcile_health(
                    &token,
                    "BBO checkpoint state changed",
                    received_at,
                    local_now,
                ) {
                    immediate.push(event);
                }
            }
        }
        let bbo_change_snapshots = immediate
            .iter()
            .filter(|event| matches!(event, MarketEvent::OrderBook(_)))
            .count();
        let _ = fields.market;
        (immediate, bbo_change_snapshots, Vec::new())
    }
}

const CAPTURE: &str = r#"{"market":"0xf197a6db9bdafce571580cabc6b0a125b306f0b9618dcebc343a038b2dfcd783", "price_changes":[{"asset_id":"45380857415000830262108920573840602994153126668582394949267342899022532123099", "price":"0.52", "size":"0", "side":"BUY", "hash":"58ad724f5f1f003dea71da79a074a2f90a1a2bca", "best_bid":"0.51", "best_ask":"0.54"}, {"asset_id":"34749117746892248233780652186823694082704908247421241024207621541426665624369", "price":"0.48", "size":"0", "side":"SELL", "hash":"6fd40a551f9a720c94d465dd9e86569c3ee8317f", "best_bid":"0.46", "best_ask":"0.49"}], "timestamp":"1789565389600", "event_type":"price_change"}"#;

fn captured_tokens() -> Vec<String> {
    let value: serde_json::Value = serde_json::from_str(CAPTURE).unwrap();
    value["price_changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["asset_id"].as_str().unwrap().to_owned())
        .collect()
}

fn seed_tokens(tokens: &[String], now: Instant) -> ClobLocalBooks {
    let specs: Vec<_> = tokens
        .chunks(2)
        .enumerate()
        .map(|(i, pair)| CanonicalEventSpec {
            condition_id: format!("condition-{i}"),
            up_token: pair[0].clone(),
            down_token: pair[1].clone(),
            tick_size: 0.01,
        })
        .collect();
    seed_specs(&specs, tokens, now)
}

fn seed_specs(specs: &[CanonicalEventSpec], tokens: &[String], now: Instant) -> ClobLocalBooks {
    let mut books = ClobLocalBooks::new(specs);
    let mut rows = Vec::new();
    for (i, token) in tokens.iter().enumerate() {
        let (bids, asks) = if i % 2 == 0 {
            (
                serde_json::json!([{"price":"0.52","size":"10"},{"price":"0.51","size":"11"}]),
                serde_json::json!([{"price":"0.54","size":"12"}]),
            )
        } else {
            (
                serde_json::json!([{"price":"0.46","size":"12"}]),
                serde_json::json!([{"price":"0.48","size":"10"},{"price":"0.49","size":"11"}]),
            )
        };
        rows.push(serde_json::json!({"event_type":"book", "asset_id":token,
            "bids":bids,"asks":asks,"timestamp":"1789565389599"}));
    }
    let batch = process_clob_frame(
        &serde_json::to_string(&rows).unwrap(),
        &mut books,
        tokens,
        now,
        1_789_565_389_599_000_000,
    );
    assert!(batch.diagnostics.is_empty());
    books
}

fn run_apply(
    books: &mut ClobLocalBooks,
    frame: &str,
    tokens: &[String],
    now: Instant,
    old: bool,
) -> (
    (Vec<MarketEvent>, usize, Vec<String>),
    ClobWireCounters,
    Vec<ClobDiagnostic>,
) {
    let fields: PriceChangeFields<'_> = serde_json::from_str(frame).unwrap();
    let mut counters = ClobWireCounters::default();
    let mut diagnostics = Vec::new();
    let result = if old {
        books.apply_price_change_before_scratch(
            fields,
            now,
            1_789_565_389_600_000_000,
            &mut counters,
            &mut diagnostics,
            tokens,
        )
    } else {
        books.apply_price_change(
            fields,
            now,
            1_789_565_389_600_000_000,
            &mut counters,
            &mut diagnostics,
            tokens,
        )
    };
    (result, counters, diagnostics)
}

fn assert_same_state(a: &ClobLocalBooks, b: &ClobLocalBooks) {
    assert_eq!(a.wire_sequence, b.wire_sequence);
    assert_eq!(a.token_books.len(), b.token_books.len());
    for (token, x) in &a.token_books {
        let y = &b.token_books[token];
        assert_eq!(x.bids, y.bids);
        assert_eq!(x.asks, y.asks);
        assert_eq!(x.exchange_timestamp_ns, y.exchange_timestamp_ns);
        assert_eq!(x.wire_sequence, y.wire_sequence);
        assert_eq!(x.dirty_since, y.dirty_since);
    }
    assert_eq!(a.canonical_versions, b.canonical_versions);
    assert_eq!(a.quote_versions, b.quote_versions);
    assert_eq!(a.health_states, b.health_states);
    assert_eq!(
        a.pending_health_recoveries.len(),
        b.pending_health_recoveries.len()
    );
    for (condition, pending) in &a.pending_health_recoveries {
        let other = &b.pending_health_recoveries[condition];
        assert_eq!(pending.due_at, other.due_at);
        assert_eq!(pending.reason, other.reason);
    }
    assert_eq!(a.pending_quotes.len(), b.pending_quotes.len());
    for (token, pending) in &a.pending_quotes {
        assert_eq!(
            format!("{pending:?}"),
            format!("{:?}", b.pending_quotes[token])
        );
    }
    assert_eq!(a.quarantined_tokens, b.quarantined_tokens);
    assert_eq!(a.degraded_tokens, b.degraded_tokens);
    assert_eq!(a.pending_bbo.len(), b.pending_bbo.len());
    for (token, pending) in &a.pending_bbo {
        assert_eq!(
            format!("{pending:?}"),
            format!("{:?}", b.pending_bbo[token])
        );
    }
    for (condition, snapshot) in &a.canonical_books {
        assert_eq!(
            format!("{snapshot:?}"),
            format!("{:?}", b.canonical_books[condition])
        );
    }
}

fn compare_apply(
    a: &mut ClobLocalBooks,
    b: &mut ClobLocalBooks,
    frame: &str,
    tokens: &[String],
    now: Instant,
) {
    let old = run_apply(a, frame, tokens, now, true);
    let new = run_apply(b, frame, tokens, now, false);
    assert_eq!(format!("{old:?}"), format!("{new:?}"));
    assert_same_state(a, b);
}

#[test]
fn captured_delete_frame_matches_old_apply_and_replay() {
    let tokens = captured_tokens();
    let now = Instant::now();
    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    compare_apply(&mut old, &mut new, CAPTURE, &tokens, now);
    assert!(!new.token_books[&tokens[0]]
        .bids
        .contains_key(&Decimal::new(52, 2)));
    assert!(!new.token_books[&tokens[1]]
        .asks
        .contains_key(&Decimal::new(48, 2)));
    assert!(new.token_books[&tokens[1]].wire_sequence > new.token_books[&tokens[0]].wire_sequence);
    compare_apply(&mut old, &mut new, CAPTURE, &tokens, now);
    compare_apply(
        &mut old,
        &mut new,
        &CAPTURE.replace("1789565389600", "1789565389500"),
        &tokens,
        now,
    );
}

#[test]
fn duplicate_interleaved_tokens_keep_first_before_last_delta_and_wire_order() {
    let tokens = captured_tokens();
    let now = Instant::now();
    let mut frame: serde_json::Value = serde_json::from_str(CAPTURE).unwrap();
    let changes = frame["price_changes"].as_array_mut().unwrap();
    let mut up = changes[0].clone();
    up["price"] = "0.50".into();
    up["size"] = "7".into();
    let mut down = changes[1].clone();
    down["price"] = "0.50".into();
    down["size"] = "7".into();
    changes.push(up.clone());
    changes.push(down);
    up["size"] = "9".into();
    changes.push(up);
    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    compare_apply(&mut old, &mut new, &frame.to_string(), &tokens, now);
    assert_eq!(
        new.token_books[&tokens[0]].bids[&Decimal::new(50, 2)],
        Decimal::from(9)
    );
    assert!(new.token_books[&tokens[0]].wire_sequence > new.token_books[&tokens[1]].wire_sequence);
}

#[test]
fn mismatched_off_tick_invalid_side_and_recovery_preserve_health_semantics() {
    let tokens = captured_tokens();
    let now = Instant::now();
    for (field, value) in [
        ("best_bid", "0.50"),
        ("price", "0.515"),
        ("side", "UNKNOWN"),
    ] {
        let mut frame: serde_json::Value = serde_json::from_str(CAPTURE).unwrap();
        frame["price_changes"][0][field] = value.into();
        let mut old = seed_tokens(&tokens, now);
        let mut new = seed_tokens(&tokens, now);
        compare_apply(&mut old, &mut new, &frame.to_string(), &tokens, now);
        compare_apply(
            &mut old,
            &mut new,
            CAPTURE,
            &tokens,
            now + Duration::from_millis(1),
        );
        let a = old.flush_deferred_due(
            now + Duration::from_millis(20),
            1_789_565_389_620_000_000,
            &tokens,
        );
        let b = new.flush_deferred_due(
            now + Duration::from_millis(20),
            1_789_565_389_620_000_000,
            &tokens,
        );
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
        assert_same_state(&old, &new);
    }
}

#[test]
fn mixed_duplicate_fields_preserve_bbo_merge_and_per_token_stale_checks() {
    let tokens = captured_tokens();
    let now = Instant::now();
    let mut value: serde_json::Value = serde_json::from_str(CAPTURE).unwrap();
    value["price_changes"][0]["best_bid"] = "0.50".into();
    let mut later = value["price_changes"][0].clone();
    later["price"] = "0.50".into();
    later["size"] = "8".into();
    later["best_bid"] = "0.51".into();
    later.as_object_mut().unwrap().remove("best_ask");
    value["price_changes"]
        .as_array_mut()
        .unwrap()
        .push(later.clone());
    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    compare_apply(&mut old, &mut new, &value.to_string(), &tokens, now);
    assert!(
        new.pending_bbo.is_empty(),
        "last bid replaces first, missing ask preserves earlier .54"
    );
    assert_eq!(
        new.token_books[&tokens[0]].bids[&Decimal::new(50, 2)],
        Decimal::from(8)
    );

    later["side"] = "UNKNOWN".into();
    later["price"] = "0.515".into();
    value["price_changes"].as_array_mut().unwrap().push(later);
    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    compare_apply(&mut old, &mut new, &value.to_string(), &tokens, now);
    assert!(
        new.pending_bbo[&tokens[0]].awaiting_tick_change,
        "off-tick evidence remains even when the later side is rejected"
    );

    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    old.token_books
        .get_mut(&tokens[0])
        .unwrap()
        .exchange_timestamp_ns = 1_789_565_389_601_000_000;
    new.token_books
        .get_mut(&tokens[0])
        .unwrap()
        .exchange_timestamp_ns = 1_789_565_389_601_000_000;
    compare_apply(&mut old, &mut new, CAPTURE, &tokens, now);
    assert!(new.token_books[&tokens[0]]
        .bids
        .contains_key(&Decimal::new(52, 2)));
    assert!(!new.token_books[&tokens[1]]
        .asks
        .contains_key(&Decimal::new(48, 2)));
}

fn boundary_frame(tokens: &[String], n: usize) -> String {
    let source: serde_json::Value = serde_json::from_str(CAPTURE).unwrap();
    let mut frame = source.clone();
    let changes: Vec<_> = (0..n)
        .map(|i| {
            let mut v = source["price_changes"][i % 2].clone();
            v["asset_id"] = tokens[i % tokens.len()].clone().into();
            v
        })
        .collect();
    frame["price_changes"] = changes.into();
    frame.to_string()
}

fn condition_spec(condition: &str, up: &str, down: &str) -> CanonicalEventSpec {
    CanonicalEventSpec {
        condition_id: condition.into(),
        up_token: up.into(),
        down_token: down.into(),
        tick_size: 0.01,
    }
}

fn compare_condition_fixture(specs: &[CanonicalEventSpec], tokens: &[String]) {
    let now = Instant::now();
    let mut old = seed_specs(specs, tokens, now);
    let mut new = seed_specs(specs, tokens, now);
    // Alias fixtures intentionally allow several Up roles for one condition.
    // The existing health_event selector uses the first matching HashMap
    // entry. Keep its unrelated randomized iteration order identical in A/B.
    new.roles = old.roles.clone();
    let matched = boundary_frame(tokens, tokens.len());
    let mut mismatched: serde_json::Value = serde_json::from_str(&matched).unwrap();
    // Multiple identities become restrictive in the same wire batch; then
    // recover through the delayed Healthy edge. Dedup errors must not hide a
    // condition's health transition or change its recovery deadline.
    for change in mismatched["price_changes"].as_array_mut().unwrap() {
        change["best_bid"] = "0.40".into();
    }
    compare_apply(&mut old, &mut new, &mismatched.to_string(), tokens, now);
    compare_apply(
        &mut old,
        &mut new,
        &matched,
        tokens,
        now + Duration::from_millis(1),
    );
    for elapsed in [2, 20, 100] {
        let observed_at = now + Duration::from_millis(elapsed);
        let local_now = 1_789_565_389_600_000_000 + elapsed * 1_000_000;
        let a = old.flush_deferred_due(observed_at, local_now, tokens);
        let b = new.flush_deferred_due(observed_at, local_now, tokens);
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
        assert_same_state(&old, &new);
    }
}

#[test]
fn repeated_condition_with_distinct_up_tokens_keeps_one_market_identity() {
    let tokens = ["up-a", "down-a", "up-b", "down-b"].map(String::from);
    let specs = [
        condition_spec("same-condition", &tokens[0], &tokens[1]),
        condition_spec("same-condition", &tokens[2], &tokens[3]),
    ];
    let state = ClobLocalBooks::new(&specs);
    assert_eq!(
        state.roles[&tokens[0]].condition_identity,
        state.roles[&tokens[2]].condition_identity
    );
    assert!(state.roles[&tokens[0]].condition_identity.is_some());
    compare_condition_fixture(&specs, &tokens);
}

#[test]
fn shared_up_token_does_not_merge_distinct_exact_conditions() {
    let tokens = ["shared-up", "down-a", "down-b", "unmapped"].map(String::from);
    let specs = [
        condition_spec("condition", &tokens[0], &tokens[1]),
        condition_spec("Condition", &tokens[0], &tokens[2]),
    ];
    let state = ClobLocalBooks::new(&specs);
    assert_ne!(
        state.roles[&tokens[1]].condition_identity,
        state.roles[&tokens[2]].condition_identity
    );
    assert_eq!(
        state.roles[&tokens[0]].condition_identity,
        state.roles[&tokens[2]].condition_identity
    );
    compare_condition_fixture(&specs, &tokens);
}

#[test]
fn unknown_role_uses_exact_token_fallback_including_condition_alias() {
    let tokens = [
        "known-up",
        "known-down",
        "known-condition",
        "unmapped-other",
    ]
    .map(String::from);
    let specs = [condition_spec("known-condition", &tokens[0], &tokens[1])];
    let state = ClobLocalBooks::new(&specs);
    assert!(!state.roles.contains_key(&tokens[2]));
    assert!(!state.roles.contains_key(&tokens[3]));
    assert_eq!(
        state.market_key_ref(&tokens[0]),
        state.market_key_ref(&tokens[2])
    );
    assert_ne!(
        state.market_key_ref(&tokens[2]),
        state.market_key_ref(&tokens[3])
    );
    compare_condition_fixture(&specs, &tokens);
}

#[test]
fn condition_interning_overflow_keeps_exact_identity_without_wrapping() {
    let conditions: Vec<_> = (0..=u16::MAX as usize + 2)
        .map(|i| format!("condition-{i}"))
        .collect();
    let mut identities = HashMap::new();
    for (i, condition) in conditions.iter().enumerate() {
        assert_eq!(
            intern_clob_condition_identity(&mut identities, condition),
            u16::try_from(i).ok()
        );
    }
    assert_eq!(identities.len(), conditions.len());
    assert_eq!(
        intern_clob_condition_identity(&mut identities, &conditions[0]),
        Some(0)
    );
    assert_eq!(
        intern_clob_condition_identity(&mut identities, &conditions[u16::MAX as usize]),
        Some(u16::MAX)
    );
    assert_eq!(
        intern_clob_condition_identity(&mut identities, &conditions[u16::MAX as usize + 1]),
        None
    );
    assert_eq!(identities.len(), conditions.len());
}

#[test]
fn all_64_distinct_tokens_apply_without_truncation_and_65_fail_before_mutation() {
    let tokens: Vec<_> = (0..64).map(|i| format!("token-{i:02}")).collect();
    let now = Instant::now();
    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    let frame = boundary_frame(&tokens, 64);
    compare_apply(&mut old, &mut new, &frame, &tokens, now);
    for (i, token) in tokens.iter().enumerate() {
        let book = &new.token_books[token];
        assert_eq!(book.wire_sequence, 65 + i as u64);
    }
    let before_sequence = new.wire_sequence;
    let invalid = boundary_frame(&tokens, 65);
    assert!(serde_json::from_str::<PriceChangeFields<'_>>(&invalid).is_err());
    let batch = process_clob_frame(&invalid, &mut new, &tokens, now, 1_789_565_389_600_000_000);
    assert_eq!(batch.wire.parse_errors, 1);
    assert!(batch.events.is_empty());
    assert_eq!(new.wire_sequence, before_sequence);
    assert_same_state(&old, &new);
}

#[test]
fn unseeded_reconnect_and_other_owner_remain_isolated() {
    let tokens = captured_tokens();
    let now = Instant::now();
    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    let isolated = seed_tokens(&tokens, now);
    old.token_books.remove(&tokens[1]);
    new.token_books.remove(&tokens[1]);
    compare_apply(&mut old, &mut new, CAPTURE, &tokens, now);
    assert!(isolated.token_books[&tokens[0]]
        .bids
        .contains_key(&Decimal::new(52, 2)));
    assert!(isolated.token_books[&tokens[1]]
        .asks
        .contains_key(&Decimal::new(48, 2)));
    let seed = serde_json::json!({"event_type":"book","asset_id":tokens[1],"bids":[{"price":"0.46","size":"12"}],"asks":[{"price":"0.49","size":"11"}],"timestamp":"1789565389601"}).to_string();
    process_clob_frame(&seed, &mut old, &tokens, now, 1_789_565_389_601_000_000);
    process_clob_frame(&seed, &mut new, &tokens, now, 1_789_565_389_601_000_000);
    compare_apply(
        &mut old,
        &mut new,
        &CAPTURE.replace("1789565389600", "1789565389602"),
        &tokens,
        now,
    );
}

fn restore_deleted_levels(books: &mut ClobLocalBooks, tokens: &[String]) {
    for (i, token) in tokens.iter().enumerate() {
        let book = books.token_books.get_mut(token).unwrap();
        if i % 2 == 0 {
            book.bids.insert(Decimal::new(52, 2), Decimal::from(10));
        } else {
            book.asks.insert(Decimal::new(48, 2), Decimal::from(10));
        }
    }
}

#[test]
fn captured_frame_eliminates_temporary_allocations_but_reports_remaining_output_allocations() {
    let tokens = captured_tokens();
    let now = Instant::now();
    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    run_apply(&mut old, CAPTURE, &tokens, now, true);
    run_apply(&mut new, CAPTURE, &tokens, now, false);
    restore_deleted_levels(&mut old, &tokens);
    restore_deleted_levels(&mut new, &tokens);
    let old_fields = serde_json::from_str::<PriceChangeFields<'_>>(CAPTURE).unwrap();
    let new_fields = serde_json::from_str::<PriceChangeFields<'_>>(CAPTURE).unwrap();
    let mut old_wire = ClobWireCounters::default();
    let mut new_wire = ClobWireCounters::default();
    let mut old_diag = Vec::new();
    let mut new_diag = Vec::new();
    let (a, oa, ob) = clob_test_allocator::count(|| {
        old.apply_price_change_before_scratch(
            old_fields,
            now,
            1_789_565_389_600_000_000,
            &mut old_wire,
            &mut old_diag,
            &tokens,
        )
    });
    let (b, na, nb) = clob_test_allocator::count(|| {
        new.apply_price_change(
            new_fields,
            now,
            1_789_565_389_600_000_000,
            &mut new_wire,
            &mut new_diag,
            &tokens,
        )
    });
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
    assert!(
        na > 0,
        "persistent BBO/owned output still allocate and must not be reported as zero"
    );
    assert!(na < oa, "old={oa}/{ob} new={na}/{nb}");
    assert!(nb < ob);
    eprintln!("captured_price_change_apply allocations old={oa} bytes={ob} new={na} bytes={nb}; parsing and fixture reset excluded; persistent/output allocations retained");
}

#[test]
#[ignore = "bounded same-thread old/new actual apply benchmark; run serially without other build load"]
fn benchmark_actual_price_change_scratch() {
    let entry_bytes = std::mem::size_of::<PriceChangeTokenScratch<'static>>();
    let scratch_bytes = std::mem::size_of::<
        arrayvec::ArrayVec<PriceChangeTokenScratch<'static>, CLOB_PRICE_CHANGE_CAPACITY>,
    >();
    let validation_bytes =
        std::mem::size_of::<arrayvec::ArrayVec<usize, CLOB_PRICE_CHANGE_CAPACITY>>();
    let touched_bytes =
        std::mem::size_of::<arrayvec::ArrayVec<(u64, usize), CLOB_PRICE_CHANGE_CAPACITY>>();
    let reconciled_bytes =
        std::mem::size_of::<arrayvec::ArrayVec<usize, CLOB_PRICE_CHANGE_CAPACITY>>();
    eprintln!("price_change_scratch_storage entry_bytes={entry_bytes} scratch_bytes={scratch_bytes} validation_bytes={validation_bytes} touched_bytes={touched_bytes} reconciled_bytes={reconciled_bytes} total_declared_local_bytes={} (type sizes, not measured compiler stack frame; excludes existing decoded fields/output/book state)", scratch_bytes + validation_bytes + touched_bytes + reconciled_bytes);
    for (label, tokens, n) in [
        ("captured_two_deletes", captured_tokens(), 100_000usize),
        (
            "64_distinct_deletes",
            (0..64).map(|i| format!("token-{i:02}")).collect(),
            10_000usize,
        ),
    ] {
        let now = Instant::now();
        let frame = if tokens.len() == 2 {
            CAPTURE.to_owned()
        } else {
            boundary_frame(&tokens, 64)
        };
        for old in [true, false] {
            let mut books = seed_tokens(&tokens, now);
            for _ in 0..128 {
                restore_deleted_levels(&mut books, &tokens);
                run_apply(&mut books, &frame, &tokens, now, old);
            }
            let mut ns = Vec::with_capacity(n);
            let mut allocs = 0;
            let mut bytes = 0;
            for _ in 0..n {
                restore_deleted_levels(&mut books, &tokens);
                let fields = serde_json::from_str::<PriceChangeFields<'_>>(&frame).unwrap();
                let mut wire = ClobWireCounters::default();
                let mut diagnostics = Vec::new();
                let ((result, elapsed), a, b) = clob_test_allocator::count(|| {
                    let start = std::time::Instant::now();
                    let result = if old {
                        books.apply_price_change_before_scratch(
                            fields,
                            now,
                            1_789_565_389_600_000_000,
                            &mut wire,
                            &mut diagnostics,
                            &tokens,
                        )
                    } else {
                        books.apply_price_change(
                            fields,
                            now,
                            1_789_565_389_600_000_000,
                            &mut wire,
                            &mut diagnostics,
                            &tokens,
                        )
                    };
                    (result, start.elapsed().as_nanos() as u64)
                });
                std::hint::black_box(result);
                ns.push(elapsed);
                allocs += a;
                bytes += b;
            }
            ns.sort_unstable();
            eprintln!("price_change_scratch label={label} version={} boundary=already_decoded_fields_to_apply_return n={n} p50_ns={} p99_ns={} p999_ns={} max_ns={} allocations={allocs} allocated_bytes={bytes} queue_hwm=NA overflow=NA (no queue; excludes parsing, reset, output drop; system allocator test binary, not production mimalloc purge reproduction)", if old {"before"} else {"after"}, ns[n/2-1], ns[n*99/100-1], ns[n*999/1000-1], ns[n-1]);
        }
    }
}

/// One diagnostic run, not a repeat-until-pass benchmark. Keep both wall and
/// CPU samples so scheduler waiting cannot silently become an algorithm claim.
#[test]
#[ignore = "paired wall/thread-CPU diagnosis for actual capture apply; run serially"]
fn diagnose_actual_price_change_wall_and_thread_cpu() {
    fn thread_cpu_ns() -> Option<u64> {
        let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
        if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } != 0 {
            return None;
        }
        Some(
            (ts.tv_sec as u64)
                .saturating_mul(1_000_000_000)
                .saturating_add(ts.tv_nsec as u64),
        )
    }
    if thread_cpu_ns().is_none() {
        eprintln!("clob_price_change_cpu_diagnostic unavailable clock=CLOCK_THREAD_CPUTIME_ID errno={} (no CPU attribution; no diagnostic samples collected)", std::io::Error::last_os_error());
        return;
    }
    let mut resolution: libc::timespec = unsafe { std::mem::zeroed() };
    let resolution_rc =
        unsafe { libc::clock_getres(libc::CLOCK_THREAD_CPUTIME_ID, &mut resolution) };
    eprintln!("clob_price_change_cpu_clock clock=CLOCK_THREAD_CPUTIME_ID getres_rc={resolution_rc} resolution_sec={} resolution_ns={} clock_reads_outside_wall_boundary=true cpu_boundary_slightly_wider_than_wall=true", resolution.tv_sec, resolution.tv_nsec);
    let tokens = captured_tokens();
    let now = Instant::now();
    let mut old_books = seed_tokens(&tokens, now);
    let mut new_books = seed_tokens(&tokens, now);
    for _ in 0..256 {
        restore_deleted_levels(&mut old_books, &tokens);
        restore_deleted_levels(&mut new_books, &tokens);
        run_apply(&mut old_books, CAPTURE, &tokens, now, true);
        run_apply(&mut new_books, CAPTURE, &tokens, now, false);
    }
    let n = 100_000;
    let mut walls = [Vec::with_capacity(n), Vec::with_capacity(n)];
    let mut cpus = [Vec::with_capacity(n), Vec::with_capacity(n)];
    let mut allocations = [0usize; 2];
    let mut allocated_bytes = [0usize; 2];
    let mut tail_counts = [0usize; 2];
    // Bounded diagnostic storage, outside every measured boundary. Report
    // omitted tails explicitly if a severely stalled host reaches this cap.
    let mut tails = Vec::with_capacity(1_024);
    let mut tail_omitted = 0usize;
    for pair in 0..n {
        for old in if pair % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let index = usize::from(!old);
            let books = if old { &mut old_books } else { &mut new_books };
            restore_deleted_levels(books, &tokens);
            let fields = serde_json::from_str::<PriceChangeFields<'_>>(CAPTURE).unwrap();
            let mut wire = ClobWireCounters::default();
            let mut diagnostics = Vec::new();
            let ((result, wall_ns, cpu_ns), a, b) = clob_test_allocator::count(|| {
                let cpu_start = thread_cpu_ns().expect("thread CPU clock was available");
                let wall_start = std::time::Instant::now();
                let result = if old {
                    books.apply_price_change_before_scratch(
                        fields,
                        now,
                        1_789_565_389_600_000_000,
                        &mut wire,
                        &mut diagnostics,
                        &tokens,
                    )
                } else {
                    books.apply_price_change(
                        fields,
                        now,
                        1_789_565_389_600_000_000,
                        &mut wire,
                        &mut diagnostics,
                        &tokens,
                    )
                };
                let wall_ns = wall_start.elapsed().as_nanos() as u64;
                let cpu_ns = thread_cpu_ns()
                    .expect("thread CPU clock remained available")
                    .saturating_sub(cpu_start);
                (result, wall_ns, cpu_ns)
            });
            std::hint::black_box(result);
            walls[index].push(wall_ns);
            cpus[index].push(cpu_ns);
            allocations[index] += a;
            allocated_bytes[index] += b;
            if wall_ns >= 1_000_000 {
                tail_counts[index] += 1;
                if tails.len() < 1_024 {
                    tails.push((pair, old, wall_ns, cpu_ns));
                } else {
                    tail_omitted += 1;
                }
            }
        }
    }
    for index in 0..2 {
        walls[index].sort_unstable();
        cpus[index].sort_unstable();
        let name = if index == 0 { "before" } else { "after" };
        let w = &walls[index];
        let c = &cpus[index];
        eprintln!("clob_price_change_cpu_diagnostic version={name} n={n} wall_p50_ns={} wall_p99_ns={} wall_p999_ns={} wall_max_ns={} cpu_p50_ns={} cpu_p99_ns={} cpu_p999_ns={} cpu_max_ns={} allocations={} allocated_bytes={} tails_ge_1ms={} paired_alternating_order=true queue_hwm=NA overflow=NA", w[n/2-1],w[n*99/100-1],w[n*999/1000-1],w[n-1],c[n/2-1],c[n*99/100-1],c[n*999/1000-1],c[n-1],allocations[index],allocated_bytes[index],tail_counts[index]);
    }
    for (pair, old, wall_ns, cpu_ns) in &tails {
        eprintln!("clob_price_change_cpu_tail pair={pair} version={} wall_ns={wall_ns} cpu_ns={cpu_ns} wall_minus_cpu_ns={}", if *old {"before"} else {"after"}, wall_ns.saturating_sub(*cpu_ns));
    }
    eprintln!("clob_price_change_cpu_tail_storage stored={} omitted={tail_omitted} capacity=1024 threshold_ns=1000000", tails.len());
}

#[test]
fn whole_handler_tail_triggers_without_slow_price_change() {
    let phases = ClobFramePhaseTimings {
        price_change_apply_ns: 13_000,
        event_construction_ns: 13_066_000,
        ..Default::default()
    };
    assert_eq!(
        clob_perf_trigger_stage(Duration::from_micros(13_087), phases)
            .unwrap()
            .0,
        "read_handler"
    );
    assert!(clob_perf_trigger_stage(Duration::from_micros(9_999), phases).is_none());
    let phases = ClobFramePhaseTimings {
        price_change_apply_ns: 5_000_000,
        ..Default::default()
    };
    assert_eq!(
        clob_perf_trigger_stage(Duration::from_millis(6), phases)
            .unwrap()
            .0,
        "price_change_apply"
    );
}

#[test]
fn owned_clob_batch_preserves_empty_and_nonempty_wire_order() {
    let tokens = captured_tokens();
    let now = Instant::now();
    for nonempty in [false, true] {
        let mut books = seed_tokens(&tokens, now);
        let ((incoming, _, _), _, _) = run_apply(&mut books, CAPTURE, &tokens, now, false);
        assert!(!incoming.is_empty());
        let mut old = if nonempty {
            incoming.clone()
        } else {
            Vec::new()
        };
        let mut new = old.clone();
        let storage = incoming.as_ptr();
        for event in incoming.clone() {
            push_latest_order_book(&mut old, event);
        }
        append_canonical_clob_events(&mut new, incoming);
        if !nonempty {
            assert_eq!(new.as_ptr(), storage);
        }
        assert_eq!(format!("{old:?}"), format!("{new:?}"));
    }
}

#[test]
#[ignore = "paired actual apply + frame assembly + output destruction benchmark, serial only"]
fn benchmark_owned_clob_frame_batch() {
    fn cpu() -> u64 {
        let mut t: libc::timespec = unsafe { std::mem::zeroed() };
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut t) },
            0
        );
        t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
    }
    const N: usize = 20_000;
    let tokens = captured_tokens();
    let now = Instant::now();
    let mut old = seed_tokens(&tokens, now);
    let mut new = seed_tokens(&tokens, now);
    let mut wall = [Vec::with_capacity(N), Vec::with_capacity(N)];
    let mut cpus = [Vec::with_capacity(N), Vec::with_capacity(N)];
    let mut alloc = [0usize; 2];
    let mut bytes = [0usize; 2];
    for i in 0..N + 256 {
        for v in [i % 2, 1 - i % 2] {
            let books = if v == 0 { &mut old } else { &mut new };
            restore_deleted_levels(books, &tokens);
            let fields = serde_json::from_str::<PriceChangeFields<'_>>(CAPTURE).unwrap();
            let mut wire = ClobWireCounters::default();
            let mut diagnostics = Vec::new();
            let ((w, c), a, b) = clob_test_allocator::count(|| {
                let start = std::time::Instant::now();
                let cpu_start = cpu();
                let (events, _, repair) = books.apply_price_change(
                    fields,
                    now,
                    1_789_565_389_600_000_000,
                    &mut wire,
                    &mut diagnostics,
                    &tokens,
                );
                let mut batch = Vec::new();
                if v == 0 {
                    for event in events {
                        push_latest_order_book(&mut batch, event);
                    }
                } else {
                    append_canonical_clob_events(&mut batch, events);
                }
                std::hint::black_box(&batch);
                drop(batch);
                drop(repair);
                drop(diagnostics);
                let c = cpu().saturating_sub(cpu_start);
                (start.elapsed().as_nanos() as u64, c)
            });
            if i >= 256 {
                wall[v].push(w);
                cpus[v].push(c);
                alloc[v] += a;
                bytes[v] += b;
            }
        }
    }
    for v in 0..2 {
        wall[v].sort_unstable();
        cpus[v].sort_unstable();
        eprintln!("owned_clob_batch version={v} n={N} boundary=decoded_apply_plus_batch_assembly_and_output_drop p50_ns={} p99_ns={} p999_ns={} max_ns={} cpu_p50_ns={} cpu_p99_ns={} cpu_p999_ns={} cpu_max_ns={} allocations={} allocated_bytes={} queue_depth=0 overflow=0 allocator=System fixture_decode_and_reset_excluded=true",wall[v][N/2],wall[v][N*99/100],wall[v][N*999/1000],wall[v][N-1],cpus[v][N/2],cpus[v][N*99/100],cpus[v][N*999/1000],cpus[v][N-1],alloc[v],bytes[v]);
    }
    assert!(alloc[1] < alloc[0]);
}
