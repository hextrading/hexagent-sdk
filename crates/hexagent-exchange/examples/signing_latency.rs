//! Focused CPU benchmark; no network calls or live credentials.
use hexagent_exchange::exchange::polymarket::{signer::SignatureType, signer_v2::OrderSignerV2};
use hexagent_types::types::Side;
use std::{hint::black_box, time::Instant};

fn main() {
    const N: usize = 20_000;
    const TOKEN: &str = "50303916472381649224674364401111317755258653723694532482715411789597335197187";
    for (label, kind) in [("eoa", SignatureType::Eoa), ("poly1271", SignatureType::Poly1271)] {
        let signer = OrderSignerV2::new(
            "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            false, kind, "",
        ).unwrap().with_funder("0x1234567890123456789012345678901234567890");
        for numeric in [false, true] {
        let prepared = hexagent_types::types::PreparedToken::parse(TOKEN).unwrap();
        for _ in 0..1000 { black_box(signer.build_signed_order_dispatch(TOKEN, 0.51, 20.0, Side::Sell).unwrap()); }
        let mut samples = Vec::with_capacity(N);
        for n in 0..N {
            let started = Instant::now();
            if numeric {
                let signed = signer.build_signed_order_numeric(TOKEN, Some(&prepared), 0.30 + (n % 60) as f64 * 0.01, 20.0, Side::Sell).unwrap();
                samples.push(started.elapsed().as_nanos()); black_box(signed);
            } else {
                let signed = signer.build_signed_order_dispatch(TOKEN, 0.30 + (n % 60) as f64 * 0.01, 20.0, Side::Sell).unwrap();
                samples.push(started.elapsed().as_nanos()); black_box(signed);
            }
        }
        samples.sort_unstable();
        println!("signing mode={label} numeric={numeric} N={N} boundary=build_signed_order median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0",
            samples[N / 2], samples[N * 99 / 100 - 1], samples[N * 999 / 1000 - 1], samples[N - 1]);
        }
    }
}
