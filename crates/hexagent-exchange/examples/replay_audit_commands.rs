// Offline-only adapter for strict order_audit commands. No strategy or live account.
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use hexagent_exchange::{
    config::BacktestConfig,
    exchange::sim::latency_record_replay::RecordReplayData,
    exchange::sim_v2::{
        ArrivalEvidence, ArrivalEvidenceReplay, BookContinuityReplay, SimV2Config, Simulator,
    },
    recorder::{MarketReplayer, ReplayOptions, ReplayTimePolicy},
    types::{
        Exchange, MarketEvent, OrderRequest, OrderSlot, OrderType, QuoteTriggerSource, Side, Signal,
    },
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
};

#[derive(Deserialize)]
struct Command {
    kind: String,
    coid: String,
    iid: String,
    event_id: String,
    token: String,
    side: String,
    order_type: String,
    price: f64,
    quantity: f64,
    post_only: bool,
    reduce_only: bool,
    fee_rate_bps: u32,
    dispatched_ns: u64,
    completed_ns: u64,
    trigger_exchange_ns: u64,
    trigger_local_ns: u64,
    epoch: u64,
    #[serde(default)]
    attempt_id: u64,
    #[serde(default)]
    observed_http_status: Option<String>,
}

fn config(
    bt: &BacktestConfig,
    data_dir: &str,
    latency_dir: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<SimV2Config> {
    let dynamic_window_rtt_by_event = if bt.sim_v2_dynamic_taker_windows {
        let data = RecordReplayData::load_dir(
            Path::new(latency_dir),
            bt.sim_latency_record_tod_bucket_secs.clamp(1, 86400) as u32,
        )?;
        Some(data.place.causal_rolling_event_quantile(
            300,
            bt.sim_v2_dynamic_window_rtt_lookback_events.max(1) as usize,
            bt.sim_v2_dynamic_window_rtt_quantile,
            bt.sim_v2_dynamic_window_rtt_cap_ms,
        ))
    } else {
        None
    };
    let cfg = SimV2Config {
        liquidity_ledger_enabled: bt.sim_v2_liquidity_ledger,
        match_time_liquidity: bt.sim_v2_match_time_liquidity,
        network_outbound_fraction_bps: bt.sim_v2_network_outbound_fraction_bps,
        market_rules_path: bt.sim_v2_market_rules_path.clone(),
        arrival_interval_audit: bt.sim_v2_arrival_interval_audit,
        historical_self_depth_path: bt.sim_v2_historical_self_depth_path.clone(),
        historical_self_depth_fraction: bt.sim_v2_historical_self_depth_fraction,
        queue_uncertainty_strength: bt.sim_v2_queue_uncertainty_strength,
        replay_arrival_time_strict: bt.sim_replay_arrival_time_strict,
        raw_server_clock: bt.sim_v2_raw_server_clock,
        strict_admission: bt.sim_v2_strict_admission,
        admission_unknown_reject: bt.sim_v2_admission_unknown_reject,
        admission_audit: bt.sim_v2_admission_audit,
        book_continuity_mode: bt
            .sim_v2_book_continuity_mode
            .parse()
            .map_err(|error: String| anyhow::anyhow!(error))?,
        data_dir: data_dir.to_owned(),
        start,
        end,
        sources: vec![("polymarket".into(), "btc-up-or-down-5m".into())],
        bootstrap_binary_open: bt.sim_v2_bootstrap_binary_open,
        binary_open_delay_ns: bt.sim_v2_binary_open_delay_ms.saturating_mul(1_000_000),
        binary_open_max_backfill_ns: bt
            .sim_v2_binary_open_max_backfill_ms
            .saturating_mul(1_000_000),
        place_p50_ms: bt.sim_latency_p50_ms as f64,
        place_p95_ms: bt.sim_latency_p95_ms as f64,
        place_p99_ms: bt.sim_latency_p99_ms as f64,
        cancel_p50_ms: bt.sim_latency_p50_ms as f64,
        cancel_p95_ms: bt.sim_latency_p95_ms as f64,
        cancel_p99_ms: bt.sim_latency_p99_ms as f64,
        rho: bt.sim_latency_correlation,
        rho_cross: bt.sim_latency_cross_correlation,
        seed: bt.sim_latency_seed,
        client_timeout_ns: bt.sim_client_timeout_ms.saturating_mul(1_000_000),
        cancel_timeout_ns: 4_000_000_000,
        reconcile_timeout_ns: 0,
        separate_taker_private_fills: true,
        wallet_usdc_by_iid: HashMap::new(),
        split_by_iid: HashMap::new(),
        ahead_frac: (bt.sim_v2_ahead_frac >= 0.0).then_some(bt.sim_v2_ahead_frac),
        dynamic_ahead_frac_strength: bt.sim_v2_dynamic_ahead_frac_strength,
        partial_depletion_queue_strength: bt.sim_v2_partial_depletion_queue_strength,
        adverse_sel_rate: bt.sim_v2_adverse_sel_rate,
        adverse_scale_ticks: bt.sim_v2_adverse_scale_ticks,
        book_through_rate: bt.sim_v2_book_through_rate,
        unexplained_depletion_exec_rate: bt.sim_v2_unexplained_depletion_exec_rate,
        depletion_trade_evidence_mult: bt.sim_v2_depletion_trade_evidence_mult,
        depletion_no_evidence_exec_frac: bt.sim_v2_depletion_no_evidence_exec_frac,
        depletion_evidence_min_shrink_frac: bt.sim_v2_depletion_evidence_min_shrink_frac,
        inferred_maker_residual_rate: bt.sim_v2_inferred_maker_residual_rate,
        inferred_maker_residual_fraction: bt.sim_v2_inferred_maker_residual_fraction,
        replay_self_depth_rate: bt.sim_v2_replay_self_depth_rate,
        replay_self_depth_fifo_replacement: bt.sim_v2_replay_self_depth_fifo_replacement,
        replay_self_taker_depth_rate: bt.sim_v2_replay_self_taker_depth_rate,
        cancel_finality_delay_frac: bt.sim_v2_cancel_finality_delay_frac,
        cancel_timing_mode: bt
            .sim_v2_cancel_timing_mode
            .parse()
            .map_err(|error: String| anyhow::anyhow!(error))?,
        cancel_processing_ns: bt
            .sim_v2_cancel_processing_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| anyhow::anyhow!("cancel processing ms overflow"))?,
        cancel_processing_fraction_bps: bt.sim_v2_cancel_processing_fraction_bps,
        execution_timing_audit: bt.sim_v2_execution_timing_audit,
        cancel_finality_counts_toward_timeout: bt.sim_v2_cancel_finality_counts_toward_timeout,
        place_ack_uncertainty_rate: bt.sim_v2_place_ack_uncertainty_rate,
        cancel_ack_uncertainty_rate: bt.sim_v2_cancel_ack_uncertainty_rate,
        private_fill_reconcile_rate: bt.sim_v2_private_fill_reconcile_rate,
        private_fill_reconcile_delay_ns: bt
            .sim_v2_private_fill_reconcile_delay_ms
            .saturating_mul(1_000_000),
        fill_markout_vn: bt.sim_v2_fill_markout_vn,
        book_fill_markout_vn: bt.sim_v2_book_fill_markout_vn,
        fill_markout_horizon_ns: bt.sim_v2_fill_markout_horizon_ms.saturating_mul(1_000_000),
        dynamic_fill_markout: bt.sim_v2_dynamic_fill_markout,
        dynamic_markout_spot_vol: bt.sim_v2_dynamic_markout_spot_vol,
        dynamic_markout_lookback_ns: bt
            .sim_v2_dynamic_markout_lookback_ms
            .saturating_mul(1_000_000),
        dynamic_markout_vol_ref_ticks: bt.sim_v2_dynamic_markout_vol_ref_ticks,
        dynamic_markout_vol_elasticity: bt.sim_v2_dynamic_markout_vol_elasticity,
        dynamic_markout_min_mult: bt.sim_v2_dynamic_markout_min_mult,
        dynamic_markout_max_mult: bt.sim_v2_dynamic_markout_max_mult,
        fill_push_mult: bt.sim_v2_fill_push_mult,
        private_fill_p50_ms: bt.sim_v2_private_fill_p50_ms,
        private_fill_p95_ms: bt.sim_v2_private_fill_p95_ms,
        private_fill_p99_ms: bt.sim_v2_private_fill_p99_ms,
        matched_cant_cancel_window_ns: bt
            .sim_matched_cant_cancel_window_ms
            .saturating_mul(1_000_000),
        per_event_rtt: None,
        taker_overhead_p50_ms: bt.sim_v2_taker_overhead_p50_ms,
        taker_overhead_p95_ms: bt.sim_v2_taker_overhead_p95_ms,
        taker_overhead_p99_ms: bt.sim_v2_taker_overhead_p99_ms,
        dynamic_taker_overhead_by_event: None,
        maker_race_rate: bt.sim_v2_maker_race_rate,
        taker_race_rate: bt.sim_v2_taker_race_rate,
        order_queue_position_strength: bt.sim_v2_order_queue_position_strength,
        exact_maker_trade_level: bt.sim_v2_exact_maker_trade_level,
        maker_toxicity_strength: bt.sim_v2_maker_toxicity_strength,
        maker_toxicity_scale_ticks: bt.sim_v2_maker_toxicity_scale_ticks,
        maker_race_horizon_ns: bt.sim_v2_maker_race_horizon_ms.saturating_mul(1_000_000),
        taker_race_horizon_ns: bt.sim_v2_taker_race_horizon_ms.saturating_mul(1_000_000),
        fold_outcomes: bt.sim_v2_fold_outcomes,
        fold_canonical_book_only: bt.sim_v2_fold_canonical_book_only,
        book_stale_after_ns: bt.sim_v2_book_stale_after_ms.saturating_mul(1_000_000),
        causal_matching: bt.sim_v2_causal_matching,
        stale_resting_exchange_only: bt.sim_v2_stale_resting_exchange_only,
        taker_comp_rate: bt.sim_v2_taker_comp_rate,
        taker_comp_window_ns: bt.sim_v2_taker_comp_window_ms.saturating_mul(1_000_000),
        taker_overlap_dedup: bt.sim_v2_taker_overlap_dedup,
        dynamic_window_rtt_by_event,
        dynamic_window_rtt_ref_ms: bt.sim_v2_dynamic_window_rtt_ref_ms,
        dynamic_race_rtt_elasticity: bt.sim_v2_dynamic_race_rtt_elasticity,
        dynamic_comp_rtt_elasticity: bt.sim_v2_dynamic_comp_rtt_elasticity,
        dynamic_window_min_mult: bt.sim_v2_dynamic_window_min_mult,
        dynamic_window_max_mult: bt.sim_v2_dynamic_window_max_mult,
        deep_queue_decay: bt.sim_v2_deep_queue_decay,
        dynamic_deep_queue_strength: bt.sim_v2_dynamic_deep_queue_strength,
        dynamic_deep_queue_min_decay: bt.sim_v2_dynamic_deep_queue_min_decay,
        // Mirror the polymarket exchange's batch flag so the sim splits
        // reprice cancels onto the cancel RTT when batching is off (the
        // live config sets use_batch_orders=false).
        use_batch_orders: false,
        // Record-replay place/cancel profiles (Some only when
        // sim_latency_calibrate_from is a directory); None → scalar CDF.
        place_profile: None,
        cancel_profile: None,
    };
    Ok(cfg)
}

fn signal(command: &Command, ordinal: usize) -> Result<Signal> {
    if command.kind == "cancel" {
        return Ok(Signal::CancelOrder {
            exchange: Exchange::Polymarket,
            client_order_id: command.coid.clone(),
            instance_id: command.iid.clone(),
            timestamp_ns: command.dispatched_ns,
        });
    }
    if command.kind != "place" {
        bail!("unsupported command kind")
    }
    let order_type = match command.order_type.as_str() {
        "Limit" => OrderType::Limit,
        "LimitMaker" => OrderType::LimitMaker,
        "Fak" => OrderType::Fak,
        "Fok" => OrderType::Fok,
        "Market" => OrderType::Market,
        other => bail!("unsupported order type {other}"),
    };
    let side = match command.side.as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => bail!("unsupported side {other}"),
    };
    if !command.price.is_finite() || !command.quantity.is_finite() || command.quantity <= 0.0 {
        bail!("invalid price/quantity")
    }
    // Synthetic diagnostic identity, not the HTTP connection slot. No strategy
    // slot table is reconstructed from incomplete historical telemetry.
    let index = (ordinal % 65_534) as u16;
    let generation = (ordinal / 65_534 + 1) as u16;
    if generation == 0 {
        bail!("diagnostic slot generation exhausted")
    }
    Ok(Signal::NewOrder(OrderRequest {
        order_slot: OrderSlot::with_generation(index, generation),
        client_order_id: command.coid.clone(),
        exchange: Exchange::Polymarket,
        symbol: command.token.clone(),
        side,
        order_type,
        price: Some(command.price),
        quantity: command.quantity,
        quote_trigger_exchange_timestamp_ns: command.trigger_exchange_ns,
        quote_trigger_local_timestamp_ns: command.trigger_local_ns,
        quote_event_id: command.event_id.clone(),
        quote_trigger_source: QuoteTriggerSource::Unknown,
        timestamp_ns: command.dispatched_ns,
        instance_id: command.iid.clone(),
        fee_rate_bps: command.fee_rate_bps,
        post_only: command.post_only,
        reduce_only: command.reduce_only,
        outcome_label: String::new(),
    }))
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 7 {
        bail!("usage: replay_audit_commands COMMANDS_JSONL EXCHANGE_TOML DATA_DIR LATENCY2_DIR OUTPUT_DIR OUTBOUND_FRACTION [CANCEL_FINALITY] [--arrival-evidence JSONL] [--book-continuity-evidence JSONL]")
    }
    let fraction: f64 = args[6].parse()?;
    if !fraction.is_finite() || !(0.0..=1.0).contains(&fraction) {
        bail!("outbound fraction must be in [0,1]")
    }
    let commands: Vec<Command> = BufReader::new(File::open(&args[1])?)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect::<Result<_>>()?;
    if commands.is_empty() {
        bail!("empty command input")
    }
    for pair in commands.windows(2) {
        if pair[0].dispatched_ns > pair[1].dispatched_ns {
            bail!("commands must be sorted by dispatch timestamp")
        }
    }
    for command in &commands {
        if command.completed_ns < command.dispatched_ns {
            bail!("negative observed RTT")
        }
    }
    let start_epoch = commands.iter().map(|c| c.epoch).min().unwrap();
    let end_epoch = commands.iter().map(|c| c.epoch).max().unwrap() + 310;
    let start = DateTime::from_timestamp(start_epoch as i64, 0).context("start timestamp")?;
    let end = DateTime::from_timestamp(end_epoch as i64, 0).context("end timestamp")?;
    let mut bt: BacktestConfig = toml::from_str(&std::fs::read_to_string(&args[2])?)?;
    let mut option_index = 7;
    if let Some(value) = args
        .get(option_index)
        .filter(|value| !value.starts_with("--"))
    {
        bt.sim_v2_cancel_finality_delay_frac = value.parse()?;
        option_index += 1;
    }
    let mut arrival_path: Option<String> = None;
    let mut continuity_path: Option<String> = None;
    while option_index < args.len() {
        let value = args
            .get(option_index + 1)
            .context("missing evidence option value")?;
        match args[option_index].as_str() {
            "--arrival-evidence" if arrival_path.is_none() => arrival_path = Some(value.clone()),
            "--book-continuity-evidence" if continuity_path.is_none() => {
                continuity_path = Some(value.clone())
            }
            other => bail!("unknown or duplicate evidence option: {other}"),
        }
        option_index += 2;
    }
    if continuity_path.is_none() && !bt.sim_v2_book_continuity_evidence_path.trim().is_empty() {
        continuity_path = Some(bt.sim_v2_book_continuity_evidence_path.clone());
    }
    let arrival_replay = arrival_path
        .as_ref()
        .map(ArrivalEvidenceReplay::from_path)
        .transpose()?;
    if let Some(replay) = &arrival_replay {
        let place_count = commands
            .iter()
            .filter(|command| command.kind == "place")
            .count();
        anyhow::ensure!(
            replay.len() == place_count,
            "arrival evidence must cover exactly the fixed place population"
        );
        let mut seen = std::collections::HashSet::with_capacity(place_count);
        for command in commands.iter().filter(|command| command.kind == "place") {
            anyhow::ensure!(
                seen.insert(command.coid.as_str()),
                "duplicate place coid in commands"
            );
            let (_, row) = replay
                .get(&command.coid)
                .context("missing place arrival evidence")?;
            anyhow::ensure!(
                row.iid == command.iid
                    && row.token == command.token
                    && row.event_epoch == command.epoch
                    && row.epoch == command.epoch
                    && row.event_id == command.event_id
                    && row.attempt_id == command.attempt_id
                    && row.dispatched_ns == command.dispatched_ns
                    && row.completed_ns == Some(command.completed_ns),
                "arrival evidence identity or timestamp mismatch for {}",
                command.coid
            );
            anyhow::ensure!(
                !command
                    .observed_http_status
                    .as_deref()
                    .is_some_and(|status| status.contains("Timeout"))
                    || row.upper_ns.is_none(),
                "HTTP timeout cannot supply an arrival upper bound for {}",
                command.coid
            );
        }
    }
    let replay_options = ReplayOptions {
        time_policy: if bt.sim_replay_arrival_time_strict {
            ReplayTimePolicy::ArrivalTimeStrict
        } else {
            ReplayTimePolicy::LegacySourceTime
        },
        bootstrap_binary_open: bt.sim_v2_bootstrap_binary_open,
        binary_open_delay_ns: bt.sim_v2_binary_open_delay_ms * 1_000_000,
        binary_open_max_backfill_ns: bt.sim_v2_binary_open_max_backfill_ms * 1_000_000,
    };
    let mut local = MarketReplayer::new_with_options(
        Path::new(&args[3]),
        "polymarket",
        "btc-up-or-down-5m",
        start,
        end,
        replay_options,
    )?;
    let mut next_local = local.next_event()?;
    let mut sim = Simulator::new(config(&bt, &args[3], &args[4], start, end)?)?;
    if let Some(path) = &continuity_path {
        let iid = &commands[0].iid;
        anyhow::ensure!(
            commands.iter().all(|command| &command.iid == iid),
            "continuity journal requires one exact owner"
        );
        sim.set_book_continuity_replay(BookContinuityReplay::from_path(path, iid)?);
    }
    sim.configure_maker_order_audit(true);
    // Observed HTTP duration already contains any server processing. This
    // diagnostic assigns its unknown internal split through the sensitivity
    // fraction and must not append the independent baseline taker overhead.
    sim.set_taker_overhead_enabled(false);
    sim.configure_selection(&bt.sim_v2_selection_mode,&bt.sim_v2_selection_model_path,bt.sim_v2_selection_strength,bt.sim_latency_seed,bt.sim_v2_selection_audit)?;
    sim.configure_selection_roles(bt.sim_v2_selection_maker_strength, bt.sim_v2_selection_taker_strength)?;
    sim.configure_maker_trade_through_recovery(bt.sim_v2_maker_trade_through_recovery)?;
    std::fs::create_dir_all(&args[5])?;
    let output = Path::new(&args[5]);
    let mut selection_writer=if bt.sim_v2_selection_audit {Some(BufWriter::new(File::create(output.join("selection_audit.jsonl"))?))} else {None};
    let mut updates = BufWriter::new(File::create(output.join("updates.jsonl"))?);
    let mut arrivals = BufWriter::new(File::create(output.join("command_arrivals.jsonl"))?);
    let mut admission = if bt.sim_v2_admission_audit {
        Some(BufWriter::new(File::create(
            output.join("admission_audit.jsonl"),
        )?))
    } else {
        None
    };
    let mut execution_timing = if bt.sim_v2_execution_timing_audit {
        Some(BufWriter::new(File::create(
            output.join("execution_timing_audit.jsonl"),
        )?))
    } else {
        None
    };
    let mut arrival_intervals = if bt.sim_v2_arrival_interval_audit {
        Some(BufWriter::new(File::create(
            output.join("arrival_interval_audit.jsonl"),
        )?))
    } else {
        None
    };
    let mut cursor = 0usize;
    let mut sim_events = 0usize;
    let mut local_events = 0usize;
    let mut update_count = 0usize;
    loop {
        let request_ns = commands
            .get(cursor)
            .map(|c| c.dispatched_ns)
            .unwrap_or(u64::MAX);
        let local_ns = next_local.as_ref().map(|r| r.0).unwrap_or(u64::MAX);
        let sim_ns = sim.peek_when().unwrap_or(u64::MAX);
        if request_ns.min(local_ns).min(sim_ns) == u64::MAX {
            break;
        }
        if sim_ns <= request_ns && sim_ns <= local_ns {
            let step_updates = sim.step();
            if let Some(writer) = admission.as_mut() {
                for row in sim.drain_admission_audit() {
                    serde_json::to_writer(&mut *writer, &row)?;
                    writeln!(writer)?;
                }
            }
            for update in step_updates {
                serde_json::to_writer(&mut updates, &update)?;
                writeln!(updates)?;
                update_count += 1;
            }
            sim_events += 1;
        } else if local_ns <= request_ns {
            let (timestamp, event) = next_local.take().unwrap();
            sim.observe_strategy_clock(timestamp);
            if let MarketEvent::OrderBook(book) = &event {
                sim.observe_local_orderbook(book, timestamp);
            }
            local_events += 1;
            next_local = local.next_event()?;
        } else {
            let command = &commands[cursor];
            let rtt = command.completed_ns - command.dispatched_ns;
            let l1 = if bt.sim_v2_network_outbound_fraction_bps == 5000 {
                (rtt as f64 * fraction).round() as u64
            } else {
                ((rtt as u128 * bt.sim_v2_network_outbound_fraction_bps as u128) / 10_000) as u64
            };
            let l2 = rtt.saturating_sub(l1);
            let cancel_timing = if command.kind == "cancel" {
                sim.cancel_timing_preview(command.dispatched_ns, l1, l2)
            } else {
                None
            };
            // Place evidence remains on its original L1. Cancel evidence is a
            // modeled partition of the same RTT, never an observed arrival.
            let nominal_arrival = command.dispatched_ns.saturating_add(l1);
            let arrival_evidence = if command.kind == "place" {
                arrival_replay
                    .as_ref()
                    .and_then(|replay| replay.get(&command.coid))
                    .map(|(row_number, record)| record.for_selected(nominal_arrival, *row_number))
                    .unwrap_or_else(|| ArrivalEvidence::modeled(nominal_arrival))
            } else {
                ArrivalEvidence::modeled(nominal_arrival)
            };
            sim.observe_strategy_clock(command.dispatched_ns);
            let recorded_timeout = matches!(
                command.observed_http_status.as_deref(),
                Some(
                    "NewOrderTimeout"
                        | "CancelOrderTimeout"
                        | "new_order_timeout"
                        | "cancel_order_timeout"
                )
            );
            let http_response_observed =
                !(bt.sim_v2_cancel_timing_mode != "legacy_l2_multiplier" && recorded_timeout);
            sim.submit_with_latency_split_and_transport_observation(
                &signal(command, cursor)?,
                command.dispatched_ns,
                l1,
                l2,
                arrival_evidence,
                http_response_observed,
            )?;
            serde_json::to_writer(
                &mut arrivals,
                &json!({"coid": command.coid, "kind": command.kind,
                "dispatch_ns": command.dispatched_ns, "observed_http_done_ns": command.completed_ns,
                "attempt_id": command.attempt_id, "iid": command.iid, "event_id": command.event_id,
                "sim_request_id": sim.last_dispatched_request_id(),
                "assumed_l1_ns": cancel_timing.map_or(l1, |t| t.network_l1_ns),
                "assumed_l2_ns": cancel_timing.map_or(l2, |t| t.network_l2_ns),
                "nominal_arrival_ns": cancel_timing.map_or(nominal_arrival, |t| t.nominal_arrival_ns),
                "arrival_evidence": cancel_timing.map_or(arrival_evidence, |t| ArrivalEvidence::modeled(t.nominal_arrival_ns)),
                "cancel_timing_mode": bt.sim_v2_cancel_timing_mode,
                "cancel_timing": cancel_timing,
                "http_response_observed": http_response_observed,
                "rtt_observation": if recorded_timeout { "right_censored_lower_bound" } else { "observed_client_interval" },
                "cancel_finality_extra_ns": if command.kind == "cancel" && cancel_timing.is_none() {(l2 as f64 * bt.sim_v2_cancel_finality_delay_frac).round() as u64} else {0}}),
            )?;
            writeln!(arrivals)?;
            cursor += 1;
        }
        if let Some(writer)=selection_writer.as_mut() {
            for row in sim.drain_selection_audit() {serde_json::to_writer(&mut *writer,&row)?;writeln!(writer)?;}
        }
        if let Some(writer) = execution_timing.as_mut() {
            for row in sim.drain_execution_timing_audit() {
                serde_json::to_writer(&mut *writer, &row)?;
                writeln!(writer)?;
            }
        }
        if let Some(writer) = arrival_intervals.as_mut() {
            for row in sim.drain_arrival_interval_audit() {
                serde_json::to_writer(&mut *writer, &row)?;
                writeln!(writer)?;
            }
        }
    }
    sim.finish_arrival_interval_audit();
    if let Some(writer) = arrival_intervals.as_mut() {
        for row in sim.drain_arrival_interval_audit() {
            serde_json::to_writer(&mut *writer, &row)?;
            writeln!(writer)?;
        }
        writer.flush()?;
    }
    if let Some(writer)=selection_writer.as_mut() {writer.flush()?;}
    std::fs::write(output.join("selection_summary.json"),serde_json::to_vec_pretty(sim.selection_stats())?)?;
    updates.flush()?;
    arrivals.flush()?;
    if let Some(writer) = execution_timing.as_mut() {
        writer.flush()?;
    }
    if let Some(writer) = admission.as_mut() {
        writer.flush()?;
    }
    let mut audit = BufWriter::new(File::create(output.join("maker_audit.jsonl"))?);
    let mut first_fill_orders: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for row in sim.maker_order_audit_rows() {
        if row.first_fill_ns > 0 {
            first_fill_orders
                .entry(row.first_fill_ns)
                .or_default()
                .push(row.coid.clone());
        }
        serde_json::to_writer(
            &mut audit,
            &json!({
                "coid": row.coid, "slug": row.slug, "iid": row.iid, "token": row.token,
                "side": row.side, "price": row.price, "quantity": row.quantity,
                "place_arrival_ns": row.place_arrival_ns, "cancel_arrival_ns": row.cancel_arrival_ns,
                "cancel_result": row.cancel_result, "rest_time_ns": row.rest_time_ns,
                "rest_qty_ns": row.rest_qty_ns, "q_init": row.q_init,
                "q_ahead_final": row.q_ahead_final, "visible_depth_at_entry": row.visible_depth_at_entry,
                "entry_mid": row.entry_mid, "queue_seq": row.queue_seq,
                "simulated_own_ahead_qty": row.simulated_own_ahead_qty,
                "replay_self_depth_credit": row.replay_self_depth_credit,
                "queue_drained_qty": row.queue_drained_qty, "trade_match_n": row.trade_match_n,
                "trade_match_qty": row.trade_match_qty, "candidate_qty": row.candidate_qty,
                "maker_toxicity_suppressed_qty": row.maker_toxicity_suppressed_qty,
                "book_through_candidate_qty": row.book_through_candidate_qty,
                "book_through_fill_qty": row.book_through_fill_qty,
                "depletion_fill_qty": row.depletion_fill_qty, "fill_qty": row.fill_qty,
                "first_fill_ns": row.first_fill_ns, "first_fill_delivery_ns": row.first_fill_delivery_ns,
                "last_fill_ns": row.last_fill_ns, "remaining_final": row.remaining_final,
            }),
        )?;
        writeln!(audit)?;
    }
    audit.flush()?;
    // Reconstruct the same server feed a second time, solely for diagnostics.
    // Exact-time public events are candidate evidence for the first maker fill;
    // matching was already complete and never consumes this output.
    let mut evidence = BufWriter::new(File::create(
        output.join("first_fill_market_evidence.jsonl"),
    )?);
    let mut evidence_rows = 0usize;
    if let Some((&last_fill, _)) = first_fill_orders.last_key_value() {
        use hexagent_exchange::exchange::sim_v2::{event::SimEvent, feed::ServerFeed};
        let mut feed = ServerFeed::new_with_clock_options(
            Path::new(&args[3]),
            &[("polymarket".into(), "btc-up-or-down-5m".into())],
            start,
            end,
            replay_options,
            bt.sim_v2_raw_server_clock,
        )?;
        while let Some((when, event)) = feed.next_server_event() {
            if when > last_fill {
                break;
            }
            let Some(coids) = first_fill_orders.get(&when) else {
                continue;
            };
            let (kind, payload, timing) = match event {
                SimEvent::ServerBook(book, timing) => ("book", serde_json::to_value(book)?, timing),
                SimEvent::ServerTrade(trade, timing) => {
                    ("trade", serde_json::to_value(trade)?, timing)
                }
                _ => continue,
            };
            serde_json::to_writer(
                &mut evidence,
                &json!({"effective_server_ns": when,
                "timing": timing,
                "first_fill_coids_at_timestamp": coids, "kind": kind, "event": payload,
                "attribution": "same reconstructed server timestamp; validate token/price/side with maker audit"}),
            )?;
            writeln!(evidence)?;
            evidence_rows += 1;
        }
    }
    evidence.flush()?;
    let summary = json!({"commands": cursor, "local_events": local_events,
        "first_fill_exact_timestamp_public_evidence_rows": evidence_rows,
        "sim_events": sim_events, "updates": update_count,
        "outbound_fraction_assumption": if bt.sim_v2_network_outbound_fraction_bps == 5000 {fraction} else {bt.sim_v2_network_outbound_fraction_bps as f64 / 10_000.0}, "window_epochs": [start_epoch, end_epoch],
        "cancel_finality_fraction": bt.sim_v2_cancel_finality_delay_frac,
        "cancel_timing_stats": sim.cancel_timing_stats(),
        "execution_timing_audit": sim.execution_timing_stats(),
        "arrival_interval_audit": sim.arrival_interval_stats(),
        "v6_fidelity": sim.v6_fidelity_stats(),
        "market_rules": sim.market_rule_stats(),
        "replay_complete": cursor == commands.len() && next_local.is_none() && sim.peek_when().is_none()
            && sim.execution_timing_stats().queued == 0 && sim.arrival_interval_stats().pending == 0
            && sim.arrival_interval_stats().queued == 0,
        "scheduler_pending_at_end": sim.peek_when().is_some(),
        "core_stats_taker_maker_reject": sim.core_stats(),
        "trade_anchor_stats": sim.trade_anchor_stats(),
        "server_time_regression_stats": sim.server_time_regression_stats(),
        "raw_older_books_dropped": sim.raw_older_books_dropped(),
        "admission_audit": sim.admission_audit_stats(),
        "book_continuity_mode": bt.sim_v2_book_continuity_mode,
        "arrival_evidence_path": arrival_path,
        "arrival_evidence_rows": arrival_replay.as_ref().map_or(0, |rows| rows.len()),
        "book_continuity_evidence_path": continuity_path,
        "raw_server_clock": bt.sim_v2_raw_server_clock,
        "replay_arrival_time_strict": bt.sim_replay_arrival_time_strict,
        "timeout_stats": sim.timeout_stats(), "cancel_finality_stats": sim.cancel_finality_stats(),
        "limitations": ["Observed private fills and HTTP statuses are targets, never forced matching inputs.",
            "Actual outbound/inbound split and cancellation finality are unobserved.",
            "Recorded whole HTTP RTT is dispatched with an explicit internal split; additional taker overhead is disabled to avoid counting it twice.",
            "Wallet gating disabled: this isolates matching of already-dispatched commands and does not reconstruct live balance availability.",
            "Synthetic diagnostic slots do not reconstruct historical strategy slots.",
            "Original recorded public tape may contain this live instance's own market impact.",
            "Maker audit contains aggregate causal evidence; it is not a full per-public-message queue transition trace."]});
    std::fs::write(
        output.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;
    println!("{}", summary);
    Ok(())
}
