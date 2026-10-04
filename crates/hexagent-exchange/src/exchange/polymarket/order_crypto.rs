//! Immutable startup-owned signing context. No global context, per-order
//! context construction, random I/O, or allocation in the signing operation.
use anyhow::{anyhow, Result};
use rand::RngCore;
use secp256k1::{Message, Secp256k1, SecretKey, SignOnly};

pub(super) struct OrderCrypto {
    context: Secp256k1<SignOnly>,
    key: SecretKey,
    legacy_key: k256::ecdsa::SigningKey,
}

const ORDER: [u8; 32] = [
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
    0xba, 0xae, 0xdc, 0xe6, 0xaf, 0x48, 0xa0, 0x3b, 0xbf, 0xd2, 0x5e, 0x8c, 0xd0, 0x36, 0x41, 0x41,
];

impl OrderCrypto {
    pub(super) fn new(bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow!("Invalid private key length: expected 32 bytes"))?;
        let key =
            SecretKey::from_byte_array(bytes).map_err(|_| anyhow!("Invalid private key scalar"))?;
        let mut context = Secp256k1::signing_only();
        // Blinding randomization is startup-only and does not change the
        // RFC6979 signature. Fail startup if the OS entropy source fails.
        let mut seed = [0; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut seed)
            .map_err(|_| anyhow!("Cannot randomize order signing context"))?;
        context.seeded_randomize(&seed);
        let legacy_key = k256::ecdsa::SigningKey::from_bytes((&bytes).into())
            .map_err(|_| anyhow!("Invalid private key scalar"))?;
        Ok(Self {
            context,
            key,
            legacy_key,
        })
    }

    pub(super) fn sign(&self, digest: &[u8; 32]) -> Result<[u8; 65]> {
        // k256 0.13 seeds RFC6979 with the unreduced prehash; libsecp
        // reduces it modulo n. Retain byte compatibility for the extremely
        // rare hash >= n as well as ordinary EIP-712 hashes.
        if digest >= &ORDER {
            let (signature, recovery) = self
                .legacy_key
                .sign_prehash_recoverable(digest)
                .map_err(|_| anyhow!("Order signing failed"))?;
            let mut result = [0; 65];
            result[..64].copy_from_slice(&signature.to_bytes());
            result[64] = recovery.to_byte() + 27;
            return Ok(result);
        }
        let (recovery, compact) = self
            .context
            .sign_ecdsa_recoverable(Message::from_digest(*digest), &self.key)
            .serialize_compact();
        let mut result = [0; 65];
        result[..64].copy_from_slice(&compact);
        result[64] = i32::from(recovery) as u8 + 27;
        Ok(result)
    }
}

impl Drop for OrderCrypto {
    fn drop(&mut self) {
        self.key.non_secure_erase();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
    use sha3::{Digest, Keccak256};

    #[test]
    fn rejects_invalid_key_without_panicking() {
        for bytes in [vec![], vec![1; 31], vec![1; 33], vec![0; 32], vec![255; 32]] {
            assert!(OrderCrypto::new(&bytes).is_err());
        }
    }

    #[test]
    fn deterministic_signatures_match_k256_and_recover_exact_owner() {
        for byte in [1, 7, 42] {
            let key = [byte; 32];
            let legacy = SigningKey::from_bytes((&key).into()).unwrap();
            let signer = OrderCrypto::new(&key).unwrap();
            let mut below = ORDER;
            below[31] -= 1;
            let mut above = ORDER;
            above[31] += 1;
            let mut digests = vec![[0; 32], [255; 32], below, ORDER, above];
            for n in 0u64..100 {
                digests.push(Keccak256::digest(n.to_be_bytes()).into());
            }
            for digest in digests {
                let signed = signer.sign(&digest).unwrap();
                let (sig, recovery) = legacy.sign_prehash_recoverable(&digest).unwrap();
                assert_eq!(&signed[..64], &sig.to_bytes()[..], "digest={digest:02x?}");
                assert_eq!(signed[64], recovery.to_byte() + 27);
                assert_eq!(signer.sign(&digest).unwrap(), signed);
                let signature = Signature::from_slice(&signed[..64]).unwrap();
                assert!(
                    signature.normalize_s().is_none(),
                    "canonical low-s signature"
                );
                let recovered = VerifyingKey::recover_from_prehash(
                    &digest,
                    &signature,
                    RecoveryId::from_byte(signed[64] - 27).unwrap(),
                )
                .unwrap();
                assert_eq!(recovered, *legacy.verifying_key());
            }
        }
    }

    #[test]
    #[ignore = "release-only: 30k old/new signing calls, startup outside boundary"]
    fn benchmark_order_crypto() {
        use std::{hint::black_box, time::Instant};
        const N: usize = 30_000;
        let key = [7; 32];
        let legacy = SigningKey::from_bytes((&key).into()).unwrap();
        let signer = OrderCrypto::new(&key).unwrap();
        let digests: Vec<[u8; 32]> = (0..N as u64)
            .map(|n| Keccak256::digest(n.to_be_bytes()).into())
            .collect();
        let mut old = Vec::with_capacity(N);
        let mut new = Vec::with_capacity(N);
        for digest in &digests {
            let start = Instant::now();
            black_box(legacy.sign_prehash_recoverable(black_box(digest)).unwrap());
            old.push(start.elapsed().as_nanos() as u64);
            let start = Instant::now();
            black_box(signer.sign(black_box(digest)).unwrap());
            new.push(start.elapsed().as_nanos() as u64);
        }
        for (name, mut values) in [("k256", old), ("startup_libsecp", new)] {
            values.sort_unstable();
            println!("{name} n={N} median_ns={} p99_ns={} p999_ns={} max_ns={} queue_depth=0 overflow=0 boundary=prehashed_recoverable_signature",
                (values[N/2-1]+values[N/2])/2, values[N*99/100-1], values[N*999/1000-1], values[N-1]);
        }
    }
}
