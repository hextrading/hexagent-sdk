// Vendored from https://github.com/robustfengbin/lighter-sdk (MIT OR Apache-2.0),
// a pure-Rust port of https://github.com/elliottech/poseidon_crypto (Apache-2.0).
// Correctness is pinned against the official Go implementation by the KAT tests
// in ../signer.rs — regenerate vectors with lighter-go if this file changes.
//! Goldilocks Field Implementation (Montgomery Form)
//!
//! The Goldilocks field is the finite field with modulus p = 2^64 - 2^32 + 1
//! This implementation uses Montgomery form for compatibility with gnark-crypto.
//! Montgomery form stores x as x * R mod p, where R = 2^64.

use std::ops::{Add, Sub, Mul, Neg};

/// Goldilocks prime: p = 2^64 - 2^32 + 1 = 0xFFFFFFFF00000001
pub const GOLDILOCKS_MODULUS: u64 = 0xFFFFFFFF00000001;

/// EPSILON = 2^32 - 1 (used in reduction)
#[allow(dead_code)]
const EPSILON: u64 = 0xFFFFFFFF;

/// R^2 mod p, where R = 2^64 (for Montgomery conversion)
/// This is pre-computed: (2^64)^2 mod (2^64 - 2^32 + 1) = 18446744065119617025
const R_SQUARE: u64 = 18446744065119617025;

/// -p^(-1) mod R = -p^(-1) mod 2^64
/// For Goldilocks: qInvNeg = 18446744069414584319
const Q_INV_NEG: u64 = 18446744069414584319;

/// ONE in Montgomery form = R mod p = 2^64 mod p = 2^32 - 1 = 4294967295
const MONT_ONE: u64 = 4294967295;

/// Goldilocks field element (stored in Montgomery form)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct GoldilocksField(pub u64);

/// Pre-computed Montgomery form constants
/// MONT_TWO = 2 * R mod p = 8589934590
const MONT_TWO: u64 = 8589934590;
/// MONT_THREE = 3 * R mod p = 12884901885
const MONT_THREE: u64 = 12884901885;

impl GoldilocksField {
    pub const ZERO: Self = Self(0);
    pub const ONE: Self = Self(MONT_ONE);
    /// TWO in Montgomery form
    pub const TWO: Self = Self(MONT_TWO);
    /// THREE in Montgomery form
    pub const THREE: Self = Self(MONT_THREE);

    /// Create from raw Montgomery form value (for pre-computed constants)
    #[inline]
    pub const fn from_raw_mont(value: u64) -> Self {
        Self(value)
    }

    /// Create a new field element from u64 (converts to Montgomery form)
    #[inline]
    pub fn new(value: u64) -> Self {
        // Convert to Montgomery form: x * R mod p = x * R^2 * R^(-1) mod p
        // We compute this by multiplying by R^2 and then using Montgomery reduction
        let reduced = Self::reduce_simple(value);
        Self::mont_mul_raw(reduced, R_SQUARE)
    }

    /// Create from u32 (converts to Montgomery form)
    #[inline]
    pub fn from_u32(value: u32) -> Self {
        Self::new(value as u64)
    }

    /// Create from i64 (converts to Montgomery form)
    #[inline]
    pub fn from_i64(value: i64) -> Self {
        if value >= 0 {
            Self::new(value as u64)
        } else {
            Self::ZERO - Self::new((-value) as u64)
        }
    }

    /// Get the value (converts from Montgomery form)
    #[inline]
    pub fn to_u64(self) -> u64 {
        // Convert from Montgomery form by multiplying by 1 (which gives x * R * 1 * R^(-1) = x)
        Self::from_mont(self.0)
    }

    /// Check if zero
    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Simple reduction modulo p (no Montgomery)
    #[inline]
    fn reduce_simple(mut x: u64) -> u64 {
        if x >= GOLDILOCKS_MODULUS {
            x -= GOLDILOCKS_MODULUS;
        }
        x
    }

    /// Montgomery multiplication using gnark's optimized algorithm
    /// Computes: a * b * R^(-1) mod q
    #[inline]
    fn mont_mul_raw(a: u64, b: u64) -> Self {
        // gnark's optimized REDC for single-limb fields:
        // hi, lo := x * y
        // m := (lo * qInvNeg) mod R
        // r := hi + hi2 + (lo != 0)
        // where hi2, _ = m * q

        let full = (a as u128) * (b as u128);
        let lo = full as u64;
        let mut hi = (full >> 64) as u64;

        // Optimization: if lo != 0, add 1 to hi
        if lo != 0 {
            hi = hi.wrapping_add(1);
        }

        // m = lo * qInvNeg (mod 2^64)
        let m = lo.wrapping_mul(Q_INV_NEG);

        // hi2, _ = m * q (only need high part)
        let hi2 = ((m as u128 * GOLDILOCKS_MODULUS as u128) >> 64) as u64;

        // r = hi2 + hi
        let (mut r, carry) = hi2.overflowing_add(hi);

        // Reduce if necessary
        if carry || r >= GOLDILOCKS_MODULUS {
            r = r.wrapping_sub(GOLDILOCKS_MODULUS);
        }

        Self(r)
    }

    /// Convert from Montgomery form to standard form
    /// Implements: z = z * R^(-1) mod q
    #[inline]
    fn from_mont(mont_val: u64) -> u64 {
        // gnark's fromMont:
        // m = z[0] * qInvNeg
        // C = madd0(m, q, z[0])  // (m * q + z[0]) >> 64
        // if C >= q: C -= q

        let m = mont_val.wrapping_mul(Q_INV_NEG);

        // hi = (m * q + mont_val) >> 64
        let product = m as u128 * GOLDILOCKS_MODULUS as u128;
        let (_lo, carry) = (product as u64).overflowing_add(mont_val);
        let mut hi = (product >> 64) as u64;
        if carry {
            hi = hi.wrapping_add(1);
        }

        // Final reduction
        if hi >= GOLDILOCKS_MODULUS {
            hi = hi.wrapping_sub(GOLDILOCKS_MODULUS);
        }

        hi
    }

    /// Square the field element
    #[inline]
    pub fn square(self) -> Self {
        self * self
    }

    /// Double the field element
    #[inline]
    pub fn double(self) -> Self {
        self + self
    }

    /// Compute x^7 (used in S-box)
    #[inline]
    pub fn pow7(self) -> Self {
        let x2 = self.square();
        let x3 = x2 * self;
        let x6 = x3.square();
        x6 * self
    }

    /// Compute modular inverse using Fermat's little theorem
    /// a^(-1) = a^(p-2) mod p
    pub fn inverse(self) -> Option<Self> {
        if self.is_zero() {
            return None;
        }

        // p - 2 = 0xFFFFFFFEFFFFFFFF
        let mut result = Self::ONE;
        let mut base = self;
        let mut exp: u64 = GOLDILOCKS_MODULUS - 2;

        while exp > 0 {
            if exp & 1 == 1 {
                result = result * base;
            }
            base = base.square();
            exp >>= 1;
        }

        Some(result)
    }

    /// Convert to little-endian bytes (converts from Montgomery form first)
    #[inline]
    pub fn to_le_bytes(self) -> [u8; 8] {
        self.to_u64().to_le_bytes()
    }

    /// Create from little-endian bytes (converts to Montgomery form)
    #[inline]
    pub fn from_le_bytes(bytes: &[u8]) -> Self {
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&bytes[..8]);
        Self::new(u64::from_le_bytes(arr))
    }
}

impl Add for GoldilocksField {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self {
        let (sum, overflow) = self.0.overflowing_add(rhs.0);
        let (result, overflow2) = if overflow {
            sum.overflowing_add(0xFFFFFFFF) // 2^64 - p = 2^32 - 1
        } else {
            (sum, false)
        };

        if overflow2 || result >= GOLDILOCKS_MODULUS {
            Self(result.wrapping_sub(GOLDILOCKS_MODULUS))
        } else {
            Self(result)
        }
    }
}

impl Sub for GoldilocksField {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self {
        let (diff, underflow) = self.0.overflowing_sub(rhs.0);
        if underflow {
            Self(diff.wrapping_add(GOLDILOCKS_MODULUS))
        } else {
            Self(diff)
        }
    }
}

impl Mul for GoldilocksField {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Self) -> Self {
        // Montgomery multiplication: (a * R) * (b * R) * R^(-1) = a * b * R
        Self::mont_mul_raw(self.0, rhs.0)
    }
}

impl Neg for GoldilocksField {
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        if self.is_zero() {
            self
        } else {
            Self(GOLDILOCKS_MODULUS - self.0)
        }
    }
}

/// Convert array of GoldilocksField to bytes (little-endian)
pub fn array_to_le_bytes(elements: &[GoldilocksField]) -> Vec<u8> {
    let mut result = Vec::with_capacity(elements.len() * 8);
    for elem in elements {
        result.extend_from_slice(&elem.to_le_bytes());
    }
    result
}

/// Convert bytes to array of GoldilocksField (little-endian)
pub fn array_from_le_bytes(bytes: &[u8]) -> Vec<GoldilocksField> {
    let mut result = Vec::with_capacity((bytes.len() + 7) / 8);
    for chunk in bytes.chunks(8) {
        let mut padded = [0u8; 8];
        padded[..chunk.len()].copy_from_slice(chunk);
        result.push(GoldilocksField::from_le_bytes(&padded));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_ops() {
        let a = GoldilocksField::new(5);
        let b = GoldilocksField::new(3);

        assert_eq!((a + b).to_u64(), 8);
        assert_eq!((a - b).to_u64(), 2);
        assert_eq!((a * b).to_u64(), 15);
    }

    #[test]
    fn test_inverse() {
        let a = GoldilocksField::new(5);
        let a_inv = a.inverse().unwrap();
        assert_eq!((a * a_inv).to_u64(), 1);
    }
}
