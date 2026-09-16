use super::*;
use serde_json::{json, Value};
use std::hint::black_box;
use std::time::Instant;

const CONDITION: &str = "0x07fd801c97d9ea8843b9e9603f70fc9b4e7f3380b9f23c113596dbf319e554c5";
const DOWN: &str = "6429758729960218843385305210371264796379759139567571381263842397737061941566";
const UP: &str = "95754735802820050529487823365125719620219860693710880917963518471093675964809";

fn pair(condition: &str, taker: &str, maker: &str) -> bool {
    condition == CONDITION && ((taker == UP && maker == DOWN) || (taker == DOWN && maker == UP))
}

fn leg(id: usize, asset: &str, side: &str, quantity: &str, price: &str) -> Value {
    json!({
        "order_id": format!("0x{id:064x}"),
        "asset_id": asset,
        "side": side,
        "matched_amount": quantity,
        "price": price,
    })
}

fn trade(asset: &str, side: &str, quantity: &str, price: &str, legs: Vec<Value>) -> Value {
    json!({
        "id": "1c940761-461d-41be-b0b4-89ce837cad86",
        "event_type": "trade",
        "market": CONDITION,
        "asset_id": asset,
        "side": side,
        "size": quantity,
        "price": price,
        "status": "MATCHED",
        "taker_order_id": format!("0x{:064x}", 999),
        "maker_orders": legs,
    })
}

fn close(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= 1e-10,
        "actual={actual:.14}, expected={expected:.14}"
    );
}

fn receipt_cases() -> Vec<(Value, f64)> {
    vec![
        // Chain gross=2.432700, quantity=15, fee=.142670. Top .16 is not VWAP.
        (
            trade(
                DOWN,
                "SELL",
                "15",
                "0.16",
                vec![
                    leg(1, DOWN, "BUY", "3.27", "0.17"),
                    leg(2, DOWN, "BUY", "11.73", "0.16"),
                ],
            ),
            2.4327,
        ),
        (
            trade(
                UP,
                "BUY",
                "15",
                "0.83",
                vec![
                    leg(3, UP, "SELL", "10", "0.83"),
                    leg(4, UP, "SELL", "5", "0.83"),
                ],
            ),
            12.45,
        ),
        (
            trade(
                UP,
                "BUY",
                "15",
                "0.85",
                vec![
                    leg(5, DOWN, "BUY", "11", "0.15"),
                    leg(6, UP, "SELL", "4", "0.85"),
                ],
            ),
            12.75,
        ),
        (
            trade(
                UP,
                "BUY",
                "15",
                "0.84",
                vec![
                    leg(7, DOWN, "BUY", "10", "0.16"),
                    leg(8, DOWN, "BUY", "5", "0.16"),
                ],
            ),
            12.6,
        ),
    ]
}

#[test]
fn exact_four_receipt_principals_use_direct_and_complementary_legs() {
    for (input, expected) in receipt_cases() {
        let normalized = normalize_taker_execution(&input, pair).unwrap();
        close(normalized.quantity, 15.0);
        close(normalized.gross_notional, expected);
    }
    let sell = normalize_taker_execution(&receipt_cases()[0].0, pair).unwrap();
    close(sell.gross_notional / sell.quantity, 0.16218);
    // Do not assert fee=sum(per-leg curve): chain disproves that hypothesis.
}

#[test]
fn buy_price_improvement_is_not_lost_to_top_level_limit() {
    let input = trade(
        UP,
        "BUY",
        "10",
        "0.30",
        vec![
            leg(1, UP, "SELL", "3", "0.28"),
            leg(2, UP, "SELL", "7", "0.29"),
        ],
    );
    let normalized = normalize_taker_execution(&input, pair).unwrap();
    close(normalized.gross_notional, 2.87);
    close(normalized.gross_notional / normalized.quantity, 0.287);
}

#[test]
fn complementary_sell_uses_merge_proceeds_and_requires_pair_proof() {
    let input = trade(
        UP,
        "SELL",
        "10",
        "0.29",
        vec![leg(1, DOWN, "SELL", "10", "0.70")],
    );
    let mut proofs = 0;
    let normalized = normalize_taker_execution(&input, |condition, taker, maker| {
        proofs += 1;
        pair(condition, taker, maker)
    })
    .unwrap();
    close(normalized.gross_notional, 3.0);
    assert_eq!(proofs, 1);
    assert!(matches!(
        normalize_taker_execution(&input, |_, _, _| false),
        Err(TakerExecutionError::InvalidPair)
    ));
    assert!(matches!(
        normalize_taker_execution(&input, |condition, _, _| condition == "another-condition"),
        Err(TakerExecutionError::InvalidPair)
    ));
}

#[test]
fn same_asset_does_not_consult_complement_registry() {
    let input = trade(
        DOWN,
        "SELL",
        "1",
        "0.16",
        vec![leg(1, DOWN, "BUY", "1", "0.17")],
    );
    let result = normalize_taker_execution(&input, |_, _, _| {
        panic!("direct match must not query a complement")
    });
    close(result.unwrap().gross_notional, 0.17);
}

#[test]
fn wrong_match_directions_and_unproven_assets_fail_closed() {
    for input in [
        trade(UP, "BUY", "1", "0.8", vec![leg(1, UP, "BUY", "1", "0.8")]),
        trade(
            UP,
            "BUY",
            "1",
            "0.8",
            vec![leg(1, DOWN, "SELL", "1", "0.2")],
        ),
        trade(
            UP,
            "BUY",
            "1",
            "0.8",
            vec![leg(1, "unrelated-token", "BUY", "1", "0.2")],
        ),
    ] {
        assert!(normalize_taker_execution(&input, pair).is_err());
    }
}

#[test]
fn leg_capacity_is_explicit_and_overflow_never_truncates_economics() {
    assert_eq!(MAX_TAKER_EXECUTION_LEGS, 64);
    let mut input = trade(
        UP,
        "BUY",
        "16",
        "0.20",
        (0..64)
            .map(|i| leg(i + 1, UP, "SELL", "0.25", "0.20"))
            .collect(),
    );
    let result = normalize_taker_execution(&input, pair).unwrap();
    close(result.quantity, 16.0);
    close(result.gross_notional, 3.2);
    input["maker_orders"]
        .as_array_mut()
        .unwrap()
        .push(leg(65, UP, "SELL", "0.25", "0.20"));
    input["size"] = json!("16.25");
    assert!(matches!(
        normalize_taker_execution(&input, pair),
        Err(TakerExecutionError::TooManyLegs)
    ));
}

#[test]
fn missing_legs_and_quantity_mismatch_cannot_become_raw_top_fallback() {
    let empty = trade(UP, "BUY", "15", "0.83", vec![]);
    assert!(matches!(
        normalize_taker_execution(&empty, pair),
        Err(TakerExecutionError::MissingLegs)
    ));
    let mut missing = empty.clone();
    missing.as_object_mut().unwrap().remove("maker_orders");
    assert!(matches!(
        normalize_taker_execution(&missing, pair),
        Err(TakerExecutionError::MissingLegs)
    ));
    let wrong_sum = trade(
        UP,
        "BUY",
        "15",
        "0.83",
        vec![leg(1, UP, "SELL", "14", "0.83")],
    );
    assert!(matches!(
        normalize_taker_execution(&wrong_sum, pair),
        Err(TakerExecutionError::QuantityMismatch)
    ));
}

#[test]
fn malformed_identity_and_non_finite_or_out_of_range_numbers_are_rejected() {
    let valid = trade(UP, "BUY", "1", "0.8", vec![leg(1, UP, "SELL", "1", "0.8")]);
    for invalid in ["NaN", "inf", "-inf", "-0.1", "0", "1", "1.1"] {
        let mut input = valid.clone();
        input["maker_orders"][0]["price"] = json!(invalid);
        assert!(
            normalize_taker_execution(&input, pair).is_err(),
            "price={invalid}"
        );
        let mut input = valid.clone();
        input["price"] = json!(invalid);
        assert!(
            normalize_taker_execution(&input, pair).is_err(),
            "raw top price={invalid}"
        );
        let complementary = trade(
            UP,
            "BUY",
            "1",
            "0.8",
            vec![leg(1, DOWN, "BUY", "1", invalid)],
        );
        assert!(
            normalize_taker_execution(&complementary, pair).is_err(),
            "complementary price={invalid}"
        );
    }
    for invalid in ["NaN", "inf", "-inf", "-1", "0"] {
        let mut input = valid.clone();
        input["maker_orders"][0]["matched_amount"] = json!(invalid);
        assert!(
            normalize_taker_execution(&input, pair).is_err(),
            "quantity={invalid}"
        );
        let mut input = valid.clone();
        input["size"] = json!(invalid);
        assert!(
            normalize_taker_execution(&input, pair).is_err(),
            "size={invalid}"
        );
    }
    for field in ["market", "asset_id", "side"] {
        let mut input = valid.clone();
        input[field] = json!("");
        assert!(
            normalize_taker_execution(&input, pair).is_err(),
            "identity={field}"
        );
    }
    let mut missing_id = valid;
    missing_id["maker_orders"][0]["order_id"] = json!("");
    assert!(matches!(
        normalize_taker_execution(&missing_id, pair),
        Err(TakerExecutionError::InvalidIdentity)
    ));
}

#[test]
fn duplicate_maker_order_is_rejected_instead_of_double_counted() {
    let repeated = leg(0xab, UP, "SELL", "1", "0.8");
    let exact = trade(
        UP,
        "BUY",
        "2",
        "0.8",
        vec![repeated.clone(), repeated.clone()],
    );
    assert!(matches!(
        normalize_taker_execution(&exact, pair),
        Err(TakerExecutionError::DuplicateLeg)
    ));
    let mut alias = repeated.clone();
    alias["order_id"] = json!(format!("  0X{:064X}  ", 0xab));
    let same_hash = trade(UP, "BUY", "2", "0.8", vec![repeated, alias]);
    assert!(matches!(
        normalize_taker_execution(&same_hash, pair),
        Err(TakerExecutionError::DuplicateLeg)
    ));
}

#[test]
fn distinct_orders_with_same_hash_bucket_survive_wraparound() {
    // These canonical 64-digit hex IDs have distinct hashes but the same
    // initial bucket126 in the production128-slot table. Four inserts cross
    // the array boundary; the last duplicate must still resolve by full ID.
    let mut input = trade(
        UP,
        "BUY",
        "4",
        "0.8",
        [235, 269, 305, 320]
            .into_iter()
            .map(|id| leg(id, UP, "SELL", "1", "0.8"))
            .collect(),
    );
    close(
        normalize_taker_execution(&input, pair)
            .unwrap()
            .gross_notional,
        3.2,
    );
    input["maker_orders"]
        .as_array_mut()
        .unwrap()
        .push(leg(320, UP, "SELL", "1", "0.8"));
    input["size"] = json!("5");
    assert!(matches!(
        normalize_taker_execution(&input, pair),
        Err(TakerExecutionError::DuplicateLeg)
    ));
}

#[test]
fn normalization_is_order_independent_and_replay_does_not_mutate_input() {
    let input = receipt_cases().remove(0).0;
    let saved = input.clone();
    let first = normalize_taker_execution(&input, pair).unwrap();
    let mut reversed = input.clone();
    reversed["maker_orders"].as_array_mut().unwrap().reverse();
    for state in ["MATCHED", "MINED", "CONFIRMED", "FAILED", "MATCHED"] {
        reversed["status"] = json!(state);
        let result = normalize_taker_execution(&reversed, pair).unwrap();
        close(result.quantity, first.quantity);
        close(result.gross_notional, first.gross_notional);
    }
    assert_eq!(input, saved);
    // This is pure-normalizer replay coverage; account status idempotence and
    // old-row provenance are tested through the real account owner separately.
}

fn legacy_top_execution(input: &Value) -> (f64, f64) {
    let number = |value: &Value| {
        value
            .as_str()
            .and_then(|v| v.parse::<f64>().ok())
            .or_else(|| value.as_f64())
            .unwrap_or(0.0)
    };
    let quantity = number(&input["size"]);
    (quantity, quantity * number(&input["price"]))
}

fn print_quantiles(name: &str, variant: &str, samples: &mut [u64]) {
    samples.sort_unstable();
    let q = |fraction: f64| samples[(samples.len() as f64 * fraction).ceil() as usize - 1];
    println!("TAKER_EXECUTION_BENCH {{\"scenario\":\"{name}\",\"variant\":\"{variant}\",\"n\":{},\"unit\":\"ns\",\"p50\":{},\"p99\":{},\"p999\":{},\"max\":{},\"capacity\":{},\"queue_depth\":null,\"overflow\":null}}", samples.len(), q(0.5), q(0.99), q(0.999), samples[samples.len() - 1], MAX_TAKER_EXECUTION_LEGS);
}

#[test]
#[ignore = "focused CPU-only old/new normalization benchmark; run explicitly with --nocapture --test-threads=1"]
fn taker_execution_before_after_benchmark() {
    const N: usize = 100_000;
    let cases = [
        (
            "single_leg",
            trade(
                DOWN,
                "SELL",
                "15",
                "0.16",
                vec![leg(1, DOWN, "BUY", "15", "0.16")],
            ),
        ),
        ("receipt_two_legs", receipt_cases().remove(0).0),
        ("mixed_complement_two_legs", receipt_cases().remove(2).0),
        (
            "maximum_64_legs",
            trade(
                UP,
                "BUY",
                "16",
                "0.20",
                (0..64)
                    .map(|i| leg(i + 1, UP, "SELL", "0.25", "0.20"))
                    .collect(),
            ),
        ),
    ];
    println!("TAKER_EXECUTION_BOUNDARY input=already-parsed-borrowed-JSON output=quantity+gross_notional; old=top-size-price-parse,multiply; new=production-normalizer-with-leg-validation-and-local-pair-proof; excludes=JSON-decode,owner-apply,queue,network,quote,dispatch; no production queue added; nearest-rank quantiles; capacities tested separately; Instant measurement overhead shared; samples allocated before measurement");
    for (name, input) in cases {
        for _ in 0..10_000 {
            black_box(legacy_top_execution(black_box(&input)));
            black_box(normalize_taker_execution(black_box(&input), pair).unwrap());
        }
        let mut old = Vec::with_capacity(N);
        let mut new = Vec::with_capacity(N);
        for index in 0..N {
            let mut measure_old = || {
                let started = Instant::now();
                black_box(legacy_top_execution(black_box(&input)));
                old.push(started.elapsed().as_nanos() as u64);
            };
            let mut measure_new = || {
                let started = Instant::now();
                black_box(normalize_taker_execution(black_box(&input), pair).unwrap());
                new.push(started.elapsed().as_nanos() as u64);
            };
            if index % 2 == 0 {
                measure_old();
                measure_new();
            } else {
                measure_new();
                measure_old();
            }
        }
        print_quantiles(name, "legacy_top", &mut old);
        print_quantiles(name, "validated_legs", &mut new);
    }
}
