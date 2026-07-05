// Vendored from https://github.com/robustfengbin/lighter-sdk (MIT OR Apache-2.0),
// a pure-Rust port of https://github.com/elliottech/poseidon_crypto (Apache-2.0).
// Correctness is pinned against the official Go implementation by the KAT tests
// in ../signer.rs — regenerate vectors with lighter-go if this file changes.
//! Goldilocks Quintic Extension Field (GFp5)
//!
//! This is a degree-5 extension of the Goldilocks field.
//! Elements are polynomials of degree < 5 over the base field,
//! with reduction modulo x^5 - 3 (the irreducible polynomial).

use super::goldilocks::GoldilocksField;
use std::ops::{Add, Sub, Mul, Neg};

/// Quintic extension of Goldilocks field
/// Represented as a[0] + a[1]*x + a[2]*x^2 + a[3]*x^3 + a[4]*x^4
/// where x^5 = 3 (i.e., reduction polynomial is x^5 - 3)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct GFp5(pub [GoldilocksField; 5]);

/// W = 3, the constant used in the extension field (x^5 = W)
/// In Montgomery form: 3 * R mod p = 12884901885
const W: GoldilocksField = GoldilocksField::THREE;

/// DTH_ROOT = 1041288259238279555, used in Frobenius
/// We compute this at runtime since the Montgomery form needs to be calculated
fn dth_root() -> GoldilocksField {
    GoldilocksField::new(1041288259238279555)
}

impl GFp5 {
    pub const ZERO: Self = Self([GoldilocksField::ZERO; 5]);
    pub const ONE: Self = Self([GoldilocksField::ONE, GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO]);
    pub const TWO: Self = Self([GoldilocksField::TWO, GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO]);

    /// Create from base field element
    pub fn from_base(elem: GoldilocksField) -> Self {
        Self([elem, GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO])
    }

    /// Create from u64
    pub fn from_u64(val: u64) -> Self {
        Self::from_base(GoldilocksField::new(val))
    }

    /// Create from array of u64
    pub fn from_u64_array(arr: [u64; 5]) -> Self {
        Self([
            GoldilocksField::new(arr[0]),
            GoldilocksField::new(arr[1]),
            GoldilocksField::new(arr[2]),
            GoldilocksField::new(arr[3]),
            GoldilocksField::new(arr[4]),
        ])
    }

    /// Convert to u64 array
    pub fn to_u64_array(self) -> [u64; 5] {
        [
            self.0[0].to_u64(),
            self.0[1].to_u64(),
            self.0[2].to_u64(),
            self.0[3].to_u64(),
            self.0[4].to_u64(),
        ]
    }

    /// Convert to base field array
    pub fn to_basefield_array(self) -> [GoldilocksField; 5] {
        self.0
    }

    /// Check if zero
    pub fn is_zero(self) -> bool {
        self.0.iter().all(|x| x.is_zero())
    }

    /// Square the element
    pub fn square(self) -> Self {
        let a = self.0;
        let w = W;
        let double_w = w + w;

        // Optimized squaring formulas from the Go implementation
        let a0s = a[0].square();
        let a1a4 = a[1] * a[4];
        let a2a3 = a[2] * a[3];
        let c0 = a0s + double_w * (a1a4 + a2a3);

        let a0_double = a[0].double();
        let c1 = a0_double * a[1] + double_w * (a[2] * a[4]) + w * a[3].square();

        let c2 = a0_double * a[2] + a[1].square() + double_w * (a[4] * a[3]);

        let a1_double = a[1].double();
        let c3 = a0_double * a[3] + a1_double * a[2] + w * a[4].square();

        let c4 = a0_double * a[4] + a1_double * a[3] + a[2].square();

        Self([c0, c1, c2, c3, c4])
    }

    /// Double the element
    pub fn double(self) -> Self {
        self + self
    }

    /// Triple the element
    pub fn triple(self) -> Self {
        let three = GoldilocksField::new(3);
        Self([
            self.0[0] * three,
            self.0[1] * three,
            self.0[2] * three,
            self.0[3] * three,
            self.0[4] * three,
        ])
    }

    /// Scalar multiplication by base field element
    pub fn scalar_mul(self, scalar: GoldilocksField) -> Self {
        Self([
            self.0[0] * scalar,
            self.0[1] * scalar,
            self.0[2] * scalar,
            self.0[3] * scalar,
            self.0[4] * scalar,
        ])
    }

    /// Frobenius endomorphism (raising to p-th power)
    pub fn frobenius(self) -> Self {
        self.repeated_frobenius(1)
    }

    /// Repeated Frobenius
    pub fn repeated_frobenius(self, count: usize) -> Self {
        if count == 0 {
            return self;
        }

        let count = count % 5;
        if count == 0 {
            return self;
        }

        // Compute z0 = dth_root^count
        let root = dth_root();
        let mut z0 = root;
        for _ in 1..count {
            z0 = z0 * root;
        }

        // Compute powers z0, z0^2, z0^3, z0^4
        let mut powers = [GoldilocksField::ONE; 5];
        for i in 1..5 {
            powers[i] = powers[i - 1] * z0;
        }

        Self([
            self.0[0] * powers[0],
            self.0[1] * powers[1],
            self.0[2] * powers[2],
            self.0[3] * powers[3],
            self.0[4] * powers[4],
        ])
    }

    /// Compute inverse (or zero if self is zero)
    pub fn inverse_or_zero(self) -> Self {
        if self.is_zero() {
            return Self::ZERO;
        }

        // Using norm-based inversion
        let d = self.frobenius();
        let e = d * d.frobenius();
        let f = e * e.repeated_frobenius(2);

        // Compute g = norm(self) in base field
        let af = self * f;
        let g = af.0[0]; // Should be in base field

        let g_inv = g.inverse().unwrap_or(GoldilocksField::ZERO);

        f.scalar_mul(g_inv)
    }

    /// Division
    pub fn div(self, rhs: Self) -> Self {
        self * rhs.inverse_or_zero()
    }

    /// Compute x^(2^n)
    pub fn exp_power_of_2(self, n: usize) -> Self {
        let mut result = self;
        for _ in 0..n {
            result = result.square();
        }
        result
    }

    /// Legendre symbol
    pub fn legendre(self) -> GoldilocksField {
        let frob1 = self.frobenius();
        let frob2 = frob1.frobenius();

        let frob1_times_frob2 = frob1 * frob2;
        let frob2_frob1_times_frob2 = frob1_times_frob2.repeated_frobenius(2);

        let xr_ext = self * frob1_times_frob2 * frob2_frob1_times_frob2;
        let xr = xr_ext.0[0];

        // xr^((p-1)/2)
        let xr31 = exp_base(xr, 1u64 << 31);
        let xr31_inv = xr31.inverse().unwrap_or(GoldilocksField::ZERO);

        let xr63 = exp_base(xr31, 1u64 << 32);

        xr63 * xr31_inv
    }

    /// Sign function for canonical square root
    pub fn sgn0(self) -> bool {
        let mut sign = false;
        let mut zero = true;
        for limb in self.0.iter() {
            let sign_i = (limb.to_u64() & 1) == 0;
            let zero_i = limb.is_zero();
            sign = sign || (zero && sign_i);
            zero = zero && zero_i;
        }
        sign
    }

    /// Try to compute square root
    pub fn sqrt(self) -> Option<Self> {
        let v = self.exp_power_of_2(31);
        let d = self * v.exp_power_of_2(32) * v.inverse_or_zero();
        // Go: e := Frobenius(Mul(d, RepeatedFrobenius(d, 2)))
        let e = (d * d.repeated_frobenius(2)).frobenius();
        let f = e.square();

        // Compute g
        let x1f4 = self.0[1] * f.0[4];
        let x2f3 = self.0[2] * f.0[3];
        let x3f2 = self.0[3] * f.0[2];
        let x4f1 = self.0[4] * f.0[1];
        let three = GoldilocksField::new(3);
        let g = self.0[0] * f.0[0] + three * (x1f4 + x2f3 + x3f2 + x4f1);

        // Check if g is a quadratic residue
        let g_sqrt = sqrt_base(g)?;

        let e_inv = e.inverse_or_zero();
        let s_fp5 = Self::from_base(g_sqrt);

        Some(s_fp5 * e_inv)
    }

    /// Canonical square root (with sign normalization)
    pub fn canonical_sqrt(self) -> Option<Self> {
        let sqrt_x = self.sqrt()?;
        if sqrt_x.sgn0() {
            Some(-sqrt_x)
        } else {
            Some(sqrt_x)
        }
    }

    /// Convert to little-endian bytes (40 bytes)
    pub fn to_le_bytes(self) -> [u8; 40] {
        let mut result = [0u8; 40];
        for (i, elem) in self.0.iter().enumerate() {
            result[i * 8..(i + 1) * 8].copy_from_slice(&elem.to_le_bytes());
        }
        result
    }

    /// Create from little-endian bytes
    pub fn from_le_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != 40 {
            return Err("GFp5 requires exactly 40 bytes");
        }
        Ok(Self([
            GoldilocksField::from_le_bytes(&bytes[0..8]),
            GoldilocksField::from_le_bytes(&bytes[8..16]),
            GoldilocksField::from_le_bytes(&bytes[16..24]),
            GoldilocksField::from_le_bytes(&bytes[24..32]),
            GoldilocksField::from_le_bytes(&bytes[32..40]),
        ]))
    }
}

/// Exponentiation in base field
fn exp_base(base: GoldilocksField, exp: u64) -> GoldilocksField {
    let mut result = GoldilocksField::ONE;
    let mut b = base;
    let mut e = exp;

    while e > 0 {
        if e & 1 == 1 {
            result = result * b;
        }
        b = b.square();
        e >>= 1;
    }

    result
}

/// Square root in base field using Tonelli-Shanks
fn sqrt_base(a: GoldilocksField) -> Option<GoldilocksField> {
    if a.is_zero() {
        return Some(GoldilocksField::ZERO);
    }

    // For Goldilocks: p = 2^64 - 2^32 + 1
    // p - 1 = 2^32 * (2^32 - 1)
    // So p ≡ 1 (mod 4), and we use Tonelli-Shanks

    // Quick check: a^((p-1)/2) should be 1
    let legendre = exp_base(a, (super::goldilocks::GOLDILOCKS_MODULUS - 1) / 2);
    if legendre.to_u64() != 1 {
        return None;
    }

    // For Goldilocks, (p+1)/4 gives us a simple formula when p ≡ 3 (mod 4)
    // But p ≡ 1 (mod 4), so we need Tonelli-Shanks

    // p - 1 = 2^32 * q where q = 2^32 - 1
    let s = 32u32;
    let q = (1u64 << 32) - 1;

    // Find a quadratic non-residue
    let mut z = GoldilocksField::new(2);
    while exp_base(z, (super::goldilocks::GOLDILOCKS_MODULUS - 1) / 2).to_u64() != super::goldilocks::GOLDILOCKS_MODULUS - 1 {
        z = z + GoldilocksField::ONE;
    }

    let mut m = s;
    let mut c = exp_base(z, q);
    let mut t = exp_base(a, q);
    let mut r = exp_base(a, (q + 1) / 2);

    loop {
        if t.to_u64() == 1 {
            return Some(r);
        }

        // Find the least i such that t^(2^i) = 1
        let mut i = 1u32;
        let mut temp = t.square();
        while temp.to_u64() != 1 {
            temp = temp.square();
            i += 1;
        }

        // b = c^(2^(m-i-1))
        let mut b = c;
        for _ in 0..(m - i - 1) {
            b = b.square();
        }

        m = i;
        c = b.square();
        t = t * c;
        r = r * b;
    }
}

impl Add for GFp5 {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self([
            self.0[0] + rhs.0[0],
            self.0[1] + rhs.0[1],
            self.0[2] + rhs.0[2],
            self.0[3] + rhs.0[3],
            self.0[4] + rhs.0[4],
        ])
    }
}

impl Sub for GFp5 {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self([
            self.0[0] - rhs.0[0],
            self.0[1] - rhs.0[1],
            self.0[2] - rhs.0[2],
            self.0[3] - rhs.0[3],
            self.0[4] - rhs.0[4],
        ])
    }
}

impl Mul for GFp5 {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        let a = self.0;
        let b = rhs.0;
        let w = W;

        // Multiply polynomials and reduce mod x^5 - 3
        let a0b0 = a[0] * b[0];
        let a1b4 = a[1] * b[4];
        let a2b3 = a[2] * b[3];
        let a3b2 = a[3] * b[2];
        let a4b1 = a[4] * b[1];
        let c0 = a0b0 + w * (a1b4 + a2b3 + a3b2 + a4b1);

        let a0b1 = a[0] * b[1];
        let a1b0 = a[1] * b[0];
        let a2b4 = a[2] * b[4];
        let a3b3 = a[3] * b[3];
        let a4b2 = a[4] * b[2];
        let c1 = a0b1 + a1b0 + w * (a2b4 + a3b3 + a4b2);

        let a0b2 = a[0] * b[2];
        let a1b1 = a[1] * b[1];
        let a2b0 = a[2] * b[0];
        let a3b4 = a[3] * b[4];
        let a4b3 = a[4] * b[3];
        let c2 = a0b2 + a1b1 + a2b0 + w * (a3b4 + a4b3);

        let a0b3 = a[0] * b[3];
        let a1b2 = a[1] * b[2];
        let a2b1 = a[2] * b[1];
        let a3b0 = a[3] * b[0];
        let a4b4 = a[4] * b[4];
        let c3 = a0b3 + a1b2 + a2b1 + a3b0 + w * a4b4;

        let a0b4 = a[0] * b[4];
        let a1b3 = a[1] * b[3];
        let a2b2 = a[2] * b[2];
        let a3b1 = a[3] * b[1];
        let a4b0 = a[4] * b[0];
        let c4 = a0b4 + a1b3 + a2b2 + a3b1 + a4b0;

        Self([c0, c1, c2, c3, c4])
    }
}

impl Neg for GFp5 {
    type Output = Self;

    fn neg(self) -> Self {
        Self([
            -self.0[0],
            -self.0[1],
            -self.0[2],
            -self.0[3],
            -self.0[4],
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gfp5_basic() {
        let a = GFp5::from_u64(5);
        let b = GFp5::from_u64(3);

        let sum = a + b;
        assert_eq!(sum.0[0].to_u64(), 8);

        let prod = a * b;
        assert_eq!(prod.0[0].to_u64(), 15);
    }

    #[test]
    fn test_gfp5_inverse() {
        let a = GFp5::from_u64(5);
        let a_inv = a.inverse_or_zero();
        let result = a * a_inv;
        assert_eq!(result.0[0].to_u64(), 1);
        for i in 1..5 {
            assert!(result.0[i].is_zero());
        }
    }
}
