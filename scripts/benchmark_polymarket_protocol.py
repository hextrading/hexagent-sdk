#!/usr/bin/env python3
"""Compile the production token-cache/selector/digest code as a small -O harness.

Uses a prebuilt release sha3 rlib. Excludes ECDSA, serialization, queues and
network; this is a focused CPU regression check, not end-to-end certification.
Source is extracted from this checkout, not a hand-maintained duplicate.
"""
from pathlib import Path
import hashlib
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def block(source, marker):
    start = source.index(marker)
    opening = source.index('{', start)
    depth = 1
    end = opening + 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[start:end]


def main():
    order = (ROOT/'crates/hexagent-types/src/types/order.rs').read_text()
    signer = (ROOT/'crates/hexagent-exchange/src/exchange/polymarket/signer_v2.rs').read_text()
    parts = [block(order, 'pub struct PreparedToken'), block(order, 'impl PreparedToken'),
             block(signer, 'fn domain_for_prepared'), block(signer, 'fn eip712_digest'),
             block(signer, 'fn keccak256')]
    libs = list((ROOT/'target/release/deps').glob('libsha3-*.rlib'))
    if not libs:
        raise SystemExit('Build release dependencies first (sha3 rlib missing)')
    sha3 = max(libs, key=lambda p:p.stat().st_mtime)
    code = 'use sha3::{Digest,Keccak256};\nmod types {\n' + '\n'.join(parts[:2]) + '\n}\n'
    code += 'struct Signer {domain_sep:[u8;32],domain_v3:[u8;32]}\nimpl Signer { #[inline]\n'+parts[2]+'\n}\n'
    code += '\n'.join(parts[3:])
    code += r'''
fn main() {
    use std::{hint::black_box,time::Instant};
    let signer = Signer {domain_sep:[1;32],domain_v3:[2;32]};
    let legacy = types::PreparedToken::parse("50303916472381649224674364401111317755258653723694532482715411789597335197187").unwrap();
    let modern = types::PreparedToken::parse("452312848583266388373324160190187140051835877600158453279131187530910662912").unwrap();
    let hash=[0x42;32];
    const N:usize=200_000;
    let mut before=vec![0u64;N]; let mut after=vec![0u64;N];
    for i in 0..N {
        let token=black_box(if i%2==0 {&legacy} else {&modern});
        let start=Instant::now();
        black_box(eip712_digest(black_box(&signer.domain_sep),black_box(&hash)));
        before[i]=start.elapsed().as_nanos() as u64;
        let start=Instant::now();
        black_box(eip712_digest(&signer.domain_for_prepared(token),black_box(&hash)));
        after[i]=start.elapsed().as_nanos() as u64;
    }
    for (name,times) in [("before_cached_v1",&mut before),("after_prepared_v1_v2",&mut after)] {
        times.sort_unstable();
        println!("{name} n={N} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=NA overflow=NA boundary=domain_selection_plus_eip712_digest",times[N/2],times[N*99/100],times[N*999/1000],times[N-1]);
    }
}
'''
    print('production_source_sha256='+hashlib.sha256('\n'.join(parts).encode()).hexdigest(), flush=True)
    with tempfile.TemporaryDirectory(prefix='polymarket-protocol-bench-') as tmp:
        path=Path(tmp)/'main.rs'; path.write_text(code)
        binary=Path(tmp)/'bench'
        subprocess.run(['rustc','--edition=2021','-O','-C','codegen-units=1','-A','dead_code',
                        '--extern',f'sha3={sha3}','-L',f'dependency={sha3.parent}',str(path),'-o',str(binary)],check=True)
        subprocess.run([str(binary)],check=True)


if __name__ == '__main__':
    main()
