// Vendored from https://github.com/robustfengbin/lighter-sdk (MIT OR Apache-2.0),
// a pure-Rust port of https://github.com/elliottech/poseidon_crypto (Apache-2.0).
// Correctness is pinned against the official Go implementation by the KAT tests
// in ../signer.rs — regenerate vectors with lighter-go if this file changes.
//! Schnorr Signature Implementation for ECgFp5
//!
//! This implements Schnorr signatures using the ECgFp5 elliptic curve
//! and Poseidon2 hash function, compatible with Lighter's signing scheme.

use super::ecgfp5::{ECgFp5Point, ECgFp5Scalar, WeierstrassPoint};
use super::gfp5::GFp5;
use super::poseidon2::hash_to_quintic_extension;

/// Schnorr signature (s, e)
#[derive(Clone, Copy, Debug)]
pub struct Signature {
    pub s: ECgFp5Scalar,
    pub e: ECgFp5Scalar,
}

impl Signature {
    /// Convert signature to bytes (80 bytes: s || e)
    pub fn to_bytes(&self) -> [u8; 80] {
        let mut result = [0u8; 80];
        result[..40].copy_from_slice(&self.s.to_le_bytes());
        result[40..].copy_from_slice(&self.e.to_le_bytes());
        result
    }

    /// Create signature from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, &'static str> {
        if data.len() != 80 {
            return Err("Signature requires 80 bytes");
        }
        Ok(Self {
            s: ECgFp5Scalar::from_le_bytes(&data[..40]),
            e: ECgFp5Scalar::from_le_bytes(&data[40..]),
        })
    }
}

/// Compute public key from private key: pk = sk * G
pub fn schnorr_pk_from_sk(sk: &ECgFp5Scalar) -> GFp5 {
    ECgFp5Point::generator().mul(sk).encode()
}

/// Sign a hashed message
///
/// # Arguments
/// * `hashed_msg` - The message hash as a GFp5 element
/// * `sk` - The private key (scalar)
///
/// # Returns
/// A Schnorr signature (s, e)
pub fn schnorr_sign_hashed_message(hashed_msg: GFp5, sk: &ECgFp5Scalar) -> Signature {
    // Sample random scalar k
    let k = ECgFp5Scalar::sample_random();
    schnorr_sign_with_k(hashed_msg, sk, &k)
}

/// Sign with an explicit nonce `k` — mirrors Go's `SchnorrSignHashedMessage2`.
/// Exists so the official comparative test vectors (fixed k) can pin this
/// implementation; production signing uses the random-`k` wrapper above.
pub fn schnorr_sign_with_k(hashed_msg: GFp5, sk: &ECgFp5Scalar, k: &ECgFp5Scalar) -> Signature {
    // Compute r = k * G
    let r = ECgFp5Point::generator().mul(k).encode();

    // Compute e = H(r || hashed_msg)
    let mut pre_image = Vec::with_capacity(10);

    // Add r components (5 elements)
    for elem in r.to_basefield_array() {
        pre_image.push(elem);
    }

    // Add hashed_msg components (5 elements)
    for elem in hashed_msg.to_basefield_array() {
        pre_image.push(elem);
    }

    let e_gfp5 = hash_to_quintic_extension(&pre_image);
    let e = ECgFp5Scalar::from_gfp5(e_gfp5);

    // Compute s = k - e * sk
    let e_sk = e.mul(sk);
    let s = k.sub(&e_sk);

    Signature { s, e }
}

/// Verify a Schnorr signature
///
/// # Arguments
/// * `pub_key` - The public key as a GFp5 element
/// * `hashed_msg` - The message hash as a GFp5 element
/// * `sig` - The signature to verify
///
/// # Returns
/// true if the signature is valid
pub fn verify_signature(pub_key: &GFp5, hashed_msg: &GFp5, sig: &Signature) -> bool {
    // Decode public key to Weierstrass point
    let pk_ws = match WeierstrassPoint::decode_fp5(*pub_key) {
        Some(p) => p,
        None => return false,
    };

    // Compute r_v = s*G + e*pk using Weierstrass coordinates
    let g_ws = WeierstrassPoint::generator();
    let r_v_point = WeierstrassPoint::mul_add2(&g_ws, &pk_ws, &sig.s, &sig.e);
    let r_v = r_v_point.encode();

    // Compute e_v = H(r_v || hashed_msg)
    let mut pre_image = Vec::with_capacity(10);

    for elem in r_v.to_basefield_array() {
        pre_image.push(elem);
    }

    for elem in hashed_msg.to_basefield_array() {
        pre_image.push(elem);
    }

    let e_v_gfp5 = hash_to_quintic_extension(&pre_image);
    let e_v = ECgFp5Scalar::from_gfp5(e_v_gfp5);

    // Check e_v == e
    e_v == sig.e
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify() {
        // Generate a random private key
        let sk = ECgFp5Scalar::sample_random();

        // Compute public key
        let pk = schnorr_pk_from_sk(&sk);

        // Create a test message hash
        let msg = GFp5::from_u64_array([1, 2, 3, 4, 5]);

        // Sign
        let sig = schnorr_sign_hashed_message(msg, &sk);

        // Verify
        assert!(verify_signature(&pk, &msg, &sig), "Signature should be valid");
    }

    /// Official comparative vectors from poseidon_crypto's
    /// `TestComparativeSchnorrSignAndVerify` (signature/schnorr/schnorr_test.go
    /// @ v0.0.17): fixed (sk, msgHash, k) → expected (s, e). Pins scalar
    /// arithmetic, point mul/encode and the challenge derivation against the
    /// official Go implementation without executing it.
    #[test]
    fn test_official_comparative_vectors() {
        let sks = [
            ECgFp5Scalar::from_limbs([
                12235002942052073545, 1175977464658719998, 8536934969147463310,
                6524687619313720391, 2922072024880609112,
            ]),
            ECgFp5Scalar::from_limbs([
                14609471659974493146, 15558617123161593410, 853367204868339037,
                17594253198278631904, 368396584122947478,
            ]),
            ECgFp5Scalar::from_limbs([
                846395111423676945, 1354180063821346280, 5751371120309175011,
                4898038106472090654, 1076345918732914302,
            ]),
        ];
        let msgs = [
            GFp5::from_u64_array([
                8398652514106806347, 11069112711939986896, 9732488227085561369,
                18076754337204438535, 17155407358725346236,
            ]),
            GFp5::from_u64_array([
                14569490467507212064, 2707063505563578676, 7506743487465742335,
                12569771346154554175, 4305083698940175790,
            ]),
            GFp5::from_u64_array([
                17529153479246803593, 1743712677205511695, 4834285972617397460,
                5486672566342530358, 7254989001695704129,
            ]),
        ];
        let ks = [
            ECgFp5Scalar::from_limbs([
                5245666847777449560, 15178169970799106939, 4403065012435293749,
                15306540389399388999, 8935555081913173844,
            ]),
            ECgFp5Scalar::from_limbs([
                1980123857560067020, 10696795398834097509, 3211831869376171671,
                6194822139276031840, 3482023782412490864,
            ]),
            ECgFp5Scalar::from_limbs([
                10299597990997564957, 8547298489021408803, 12250978550108858722,
                5282281975236198197, 5328603554431393061,
            ]),
        ];
        let expected_s = [
            [6950590877883398434u64, 17178336263794770543, 11012823478139181320,
             16445091359523510936, 5882925226143600273],
            [15189311883262425203, 16924634885527914505, 11098200095411565797,
             11441434601417451505, 2245797172600273048],
            [1747989245728027396, 18083435619737379521, 18276259610811995786,
             15101757397705334408, 5007814817019340642],
        ];
        let expected_e = [
            [4544744459434870309u64, 4180764085957612004, 3024669018778978615,
             15433417688859446606, 6775027260348937828],
            [4905460437060282008, 9275377852059362729, 10383772785796962929,
             6858067464918579610, 7078247668913970626],
            [4911725746357568132, 12205663641120664338, 16433506899074513700,
             14763562571101437023, 2547950465160283358],
        ];

        for i in 0..3 {
            let sig = schnorr_sign_with_k(msgs[i], &sks[i], &ks[i]);
            assert_eq!(sig.s.0, expected_s[i], "s mismatch at vector {}", i);
            assert_eq!(sig.e.0, expected_e[i], "e mismatch at vector {}", i);
            let pk = schnorr_pk_from_sk(&sks[i]);
            assert!(verify_signature(&pk, &msgs[i], &sig), "verify failed at vector {}", i);
        }
    }

    #[test]
    fn test_invalid_signature() {
        // Generate two different private keys
        let sk1 = ECgFp5Scalar::sample_random();
        let sk2 = ECgFp5Scalar::sample_random();

        // Use pk from sk1
        let pk = schnorr_pk_from_sk(&sk1);

        // Sign with sk2
        let msg = GFp5::from_u64_array([1, 2, 3, 4, 5]);
        let sig = schnorr_sign_hashed_message(msg, &sk2);

        // Verify should fail
        assert!(!verify_signature(&pk, &msg, &sig), "Signature should be invalid");
    }
}
