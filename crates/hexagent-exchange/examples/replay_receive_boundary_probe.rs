//! Read-only verification of the strict warmup/replay visibility boundary.
//! Run: replay_receive_boundary_probe DATA_DIR START_NS CUTOFF_NS END_NS
//! The timing boundary is one MarketReplayer::next_event call, including loader
//! wait when its bounded handoff has no next batch. Hashing and stats sampling
//! happen outside that measured interval. This is an offline reader benchmark,
//! not a strategy/live quote-path latency claim.
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use hexagent_exchange::recorder::{
    replayer_stats, MarketReplayer, ReplayOptions, ReplayTimePolicy,
};
use hexagent_exchange::types::MarketEvent;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::Instant;

fn source_ns(event: &MarketEvent) -> Option<u64> {
    match event {
        MarketEvent::OrderBook(event) => Some(event.exchange_timestamp_ns),
        MarketEvent::Quote(event) => Some(event.exchange_timestamp_ns),
        MarketEvent::Trade(event) => Some(event.exchange_timestamp_ns),
        MarketEvent::SpotPrice(event) => Some(event.timestamp_ns),
        MarketEvent::TickSizeChange(event) => Some(event.exchange_timestamp_ns),
        _ => None,
    }
}

fn scan(
    directory: &Path,
    exchange: &str,
    symbol: &str,
    start_ns: u64,
    end_ns: u64,
    cutoff_ns: u64,
    hash: &mut Sha256,
) -> Result<Value> {
    let started = Instant::now();
    let mut reader = MarketReplayer::new_with_options(
        directory,
        exchange,
        symbol,
        DateTime::<Utc>::from_timestamp_nanos(start_ns as i64),
        DateTime::<Utc>::from_timestamp_nanos(end_ns as i64),
        ReplayOptions {
            time_policy: ReplayTimePolicy::ArrivalTimeStrict,
            ..ReplayOptions::default()
        },
    )?;
    let constructor_ns = started.elapsed().as_nanos();
    let mut rows = 0_u64;
    let mut previous_receive = None;
    let mut late_source_rows = 0_u64;
    let mut max_late_receive_ns = 0_u64;
    let mut max_handoff_rows = 0_u64;
    let mut max_handoff_capacity = 0_u64;
    let mut durations = Vec::with_capacity(8192);
    loop {
        let before = Instant::now();
        let next = reader.next_event()?;
        let elapsed = before.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        let Some((receive, event)) = next else {
            break;
        };
        if durations.len() >= 1_000_000 {
            return Err(anyhow!("probe row cap exceeded; choose a shorter window"));
        }
        durations.push(elapsed);
        if receive < start_ns
            || receive >= end_ns
            || previous_receive.is_some_and(|previous| receive < previous)
        {
            return Err(anyhow!("strict receive bounds/order violation {exchange}/{symbol}: {receive} [{start_ns},{end_ns})"));
        }
        previous_receive = Some(receive);
        if source_ns(&event).is_some_and(|source| source < cutoff_ns) && receive >= cutoff_ns {
            late_source_rows += 1;
            max_late_receive_ns = max_late_receive_ns.max(receive - cutoff_ns);
        }
        let payload = rmp_serde::to_vec(&event)?;
        hash.update(receive.to_le_bytes());
        hash.update((payload.len() as u64).to_le_bytes());
        hash.update(payload);
        let stats = replayer_stats();
        max_handoff_rows = max_handoff_rows.max(stats.buffered_rows);
        max_handoff_capacity = max_handoff_capacity.max(stats.buffer_capacity);
        rows += 1;
    }
    let first_next_ns = durations.first().copied().unwrap_or(0);
    durations.sort_unstable();
    let quantile = |p: f64| -> Option<u64> {
        (!durations.is_empty())
            .then(|| durations[((durations.len() as f64 * p).ceil() as usize).saturating_sub(1)])
    };
    drop(reader);
    Ok(json!({
        "rows": rows, "start_ns": start_ns, "end_ns": end_ns,
        "source_before_cutoff_received_at_or_after_cutoff": late_source_rows,
        "max_late_receive_ns": max_late_receive_ns,
        "outside_receive_window_rows": 0, "receive_regressions": 0,
        "constructor_ns": constructor_ns, "first_next_event_ns": first_next_ns,
        "elapsed_total_ns": started.elapsed().as_nanos(),
        "next_event_ns": {"n": durations.len(), "median": quantile(0.5), "p99": quantile(0.99),
            "p999": quantile(0.999), "maximum": durations.last()},
        "observed_handoff_rows_max": max_handoff_rows,
        "observed_handoff_capacity_max": max_handoff_capacity,
        "overflow": 0,
        "overflow_semantics": "existing rendezvous handoff blocks loader; decode/provenance errors abort",
        "measurement_boundary": "MarketReplayer::next_event including loader wait, excluding hash/stats; final EOF call excluded"
    }))
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 5 {
        return Err(anyhow!(
            "usage: {} DATA_DIR START_NS CUTOFF_NS END_NS",
            args[0]
        ));
    }
    let directory = Path::new(&args[1]);
    let start: u64 = args[2].parse()?;
    let cutoff: u64 = args[3].parse()?;
    let end: u64 = args[4].parse()?;
    if !(start < cutoff && cutoff < end && end <= i64::MAX as u64) {
        return Err(anyhow!(
            "require 0 <= START_NS < CUTOFF_NS < END_NS <= i64::MAX"
        ));
    }
    let mut results = Vec::new();
    for (exchange, symbol) in [
        ("binance", "BTCUSDT"),
        ("coinbase", "BTC-USD"),
        ("chainlink", "btc-usd"),
    ] {
        let mut whole_hash = Sha256::new();
        let whole = scan(
            directory,
            exchange,
            symbol,
            start,
            end,
            cutoff,
            &mut whole_hash,
        )?;
        let mut split_hash = Sha256::new();
        let warmup = scan(
            directory,
            exchange,
            symbol,
            start,
            cutoff,
            cutoff,
            &mut split_hash,
        )?;
        let replay = scan(
            directory,
            exchange,
            symbol,
            cutoff,
            end,
            cutoff,
            &mut split_hash,
        )?;
        let whole_sha = hex::encode(whole_hash.finalize());
        let split_sha = hex::encode(split_hash.finalize());
        if whole_sha != split_sha {
            return Err(anyhow!(
                "whole vs warmup+replay sequence mismatch for {exchange}/{symbol}"
            ));
        }
        results.push(
            json!({"exchange": exchange, "symbol": symbol, "whole": whole,
            "warmup": warmup, "replay": replay, "whole_sequence_sha256": whole_sha,
            "warmup_plus_replay_sequence_sha256": split_sha, "sequence_exact_match": true}),
        );
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "profile": "ArrivalTimeStrict", "cutoff_ns": cutoff, "sources": results,
            "same_receive_tie_order": "discovered file lexicographic order, then original row order; duplicates retained",
            "memory_scope": "reported rows/capacity cover existing handoff batches; Arrow/sort/scratch buffers excluded",
            "scratch_limit": "512 MiB per overlapping group before merge, approximately twice that while merging; groups deleted sequentially"
        }))?
    );
    Ok(())
}
