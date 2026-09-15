//! Compare the unchanged canonical ledger folds with live incremental queries.
//! Run: cargo run --release -p hexagent-account --example position_query_benchmark
//! Optional args: samples (default 10000), then history sizes.
use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use hexagent_account::account::position::{PositionManager, TradeStatus};
use hexagent_account::types::Side;

fn seeded(history: usize, indexed: bool) -> PositionManager {
    let mut pm = PositionManager::with_initial_quantities(
        HashMap::from([("ACTIVE".into(), 100_000.0)]), 1_000_000.0);
    for index in 0..history {
        // Keep historical symbols represented, as long-lived live ledgers do.
        let symbol = format!("HISTORY-{}", index % 256);
        let id = format!("history-{index:08}");
        let side = if index % 2 == 0 { Side::Buy } else { Side::Sell };
        let status = if index % 19 == 0 { TradeStatus::Failed } else { TradeStatus::Confirmed };
        pm.upsert_trade(&id, &symbol, side, 2.0, 0.4, status, true, 0.0, 0.0, None);
    }
    for index in 0..16 {
        pm.register_pending_order(&format!("order-{index}"), "ACTIVE",
            if index % 2 == 0 { Side::Buy } else { Side::Sell }, 0.4, 100_000.0);
    }
    if indexed {
        pm.enable_incremental_queries();
    }
    pm
}

fn sample(pm: &mut PositionManager, trade_id: &str, event: usize) {
    // Boundaries mirror the PM portion of StrategyAccount private application:
    // quantity-before, new Matched upsert, reservation update, available cash
    // and inventory refresh. Includes ledger insertion, excludes input string
    // construction, histogram work, logger/IO, and the surrounding SDK queues.
    black_box(pm.get_quantity(black_box("ACTIVE")));
    let side = if event % 2 == 0 { Side::Buy } else { Side::Sell };
    let result = pm.upsert_trade(black_box(trade_id), "ACTIVE", side, 1.0, 0.4,
        TradeStatus::Matched, true, 0.0, 0.0, None);
    let order = if side == Side::Buy { "order-0" } else { "order-1" };
    black_box(pm.apply_private_trade_reservation(order, 1.0, result.accumulator_sign));
    black_box(pm.available_cash());
    black_box(pm.available_inventory("ACTIVE"));
}

fn main() {
    let mut args = std::env::args().skip(1);
    let samples = args.next().map(|s| s.parse().unwrap()).unwrap_or(10_000usize);
    assert!(samples >= 1000, "P999 needs at least 1000 observations");
    let mut histories: Vec<usize> = args.map(|s| s.parse().unwrap()).collect();
    if histories.is_empty() { histories = vec![1000, 10_000, 50_000]; }
    let trade_ids: Vec<String> = (0..samples + 100).map(|i| format!("live-{i:08}")).collect();
    println!("mode,history_start,history_end,events,median_ns,p99_ns,p999_ns,max_ns,active_pending_orders,queue_depth,overflow");
    for history in histories {
        for indexed in [false, true] {
            let mut pm = seeded(history, indexed);
            for (event, id) in trade_ids.iter().take(100).enumerate() { sample(&mut pm, id, event); }
            let mut elapsed = Vec::with_capacity(samples);
            for (event, id) in trade_ids.iter().skip(100).enumerate() {
                let start = Instant::now();
                sample(&mut pm, id, event);
                elapsed.push(start.elapsed().as_nanos());
            }
            elapsed.sort_unstable();
            let percentile = |p: f64| elapsed[((samples as f64 * p).ceil() as usize - 1).min(samples - 1)];
            println!("{},{},{},{},{},{},{},{},{},0,0",
                if indexed { "incremental" } else { "canonical_scan" },
                history + 100, pm.trades().len(), samples,
                percentile(0.5), percentile(0.99), percentile(0.999), elapsed[samples - 1],
                pm.pending_orders().len());
        }
    }
}
