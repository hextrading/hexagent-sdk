// Vendored from https://github.com/robustfengbin/lighter-sdk (MIT OR Apache-2.0),
// a pure-Rust port of https://github.com/elliottech/poseidon_crypto (Apache-2.0).
// Correctness is pinned against the official Go implementation by the KAT tests
// in ../signer.rs — regenerate vectors with lighter-go if this file changes.
//! ECgFp5 Elliptic Curve Implementation
//!
//! An elliptic curve over the quintic extension of the Goldilocks field.
//! Curve equation: y^2 = x*(x^2 + a*x + b) where a = 2, b = 263*i (i is the generator of GFp5)
//!
//! This implements the curve used in Lighter's Schnorr signatures.

use super::gfp5::GFp5;
use super::goldilocks::GoldilocksField;
use num_bigint::BigUint;
use num_traits::Zero;

/// Curve parameter A = 2
const A: GFp5 = GFp5([GoldilocksField::TWO, GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO]);

/// B coefficient: b = 263 * i (where i is the second basis element)
const B1: u64 = 263;

/// Curve parameter B = 263*i
fn b() -> GFp5 {
    GFp5([GoldilocksField::ZERO, GoldilocksField::new(B1), GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO])
}

fn b_mul2() -> GFp5 {
    GFp5([GoldilocksField::ZERO, GoldilocksField::new(2 * B1), GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO])
}

fn b_mul4() -> GFp5 {
    GFp5([GoldilocksField::ZERO, GoldilocksField::new(4 * B1), GoldilocksField::ZERO, GoldilocksField::ZERO, GoldilocksField::ZERO])
}

/// ECgFp5 Point in projective coordinates
/// Using (x, u) fractional coordinates: for curve point (x, y), we have (x, u) = (x, x/y) = (X/Z, U/T)
#[derive(Clone, Copy, Debug)]
pub struct ECgFp5Point {
    x: GFp5,
    z: GFp5,
    u: GFp5,
    t: GFp5,
}

/// Weierstrass point in projective coordinates (X, Y, Z)
/// Represents the affine point (X/Z, Y/Z) on the curve y^2 = x^3 + ax + b
#[derive(Clone, Copy, Debug)]
pub struct WeierstrassPoint {
    x: GFp5,
    y: GFp5,
    z: GFp5,
}

/// A coefficient for Weierstrass curve form: -A^2/3 + B = -4/3 + 263*i
fn a_weierstrass() -> GFp5 {
    GFp5::from_u64_array([6148914689804861439, 263, 0, 0, 0])
}

impl WeierstrassPoint {
    /// Point at infinity
    pub fn infinity() -> Self {
        Self {
            x: GFp5::ZERO,
            y: GFp5::ONE,
            z: GFp5::ZERO,
        }
    }

    /// Generator point for Weierstrass form
    pub fn generator() -> Self {
        Self {
            x: GFp5::from_u64_array([
                11712523173042564207,
                14090224426659529053,
                13197813503519687414,
                16280770174934269299,
                15998333998318935536,
            ]),
            y: GFp5::from_u64_array([
                14639054205878357578,
                17426078571020221072,
                2548978194165003307,
                8663895577921260088,
                9793640284382595140,
            ]),
            z: GFp5::ONE,
        }
    }

    /// Check if this is the point at infinity
    pub fn is_infinity(&self) -> bool {
        self.z.is_zero()
    }

    /// Decode a GFp5 element to Weierstrass point
    pub fn decode_fp5(w: GFp5) -> Option<Self> {
        // Same quadratic solving as ECgFp5Point::decode
        let e = w.square() - A;
        let delta = e.square() - b_mul4();

        let r = delta.canonical_sqrt();
        let c = r.is_some();

        if !c && !w.is_zero() {
            return None;
        }

        let r = r.unwrap_or(GFp5::ZERO);

        let x1 = (e + r).div(GFp5::TWO);
        let x2 = (e - r).div(GFp5::TWO);

        let x1_legendre = x1.legendre();
        let x = if x1_legendre.to_u64() == 1 { x1 } else { x2 };

        // Compute y = -w * x
        let y = -(w * x);

        // Convert to Weierstrass coordinates: x_w = x + A/3
        let a_over_3 = GFp5::TWO.div(GFp5::from_u64_array([3, 0, 0, 0, 0]));
        let x_w = x + a_over_3;

        if c {
            Some(Self { x: x_w, y, z: GFp5::ONE })
        } else {
            // w == 0 maps to infinity
            Some(Self::infinity())
        }
    }

    /// Encode point to GFp5 element
    pub fn encode(&self) -> GFp5 {
        if self.is_infinity() {
            return GFp5::ZERO;
        }

        let a_over_3 = GFp5::TWO.div(GFp5::from_u64_array([3, 0, 0, 0, 0]));
        let x_orig = self.x * self.z.inverse_or_zero() - a_over_3;
        let y = self.y * self.z.inverse_or_zero();

        -y * x_orig.inverse_or_zero()
    }

    /// Point doubling
    pub fn double(&self) -> Self {
        if self.is_infinity() || self.y.is_zero() {
            return Self::infinity();
        }

        let x_affine = self.x * self.z.inverse_or_zero();
        let y_affine = self.y * self.z.inverse_or_zero();

        let three_x2 = x_affine.square() * GFp5::from_u64_array([3, 0, 0, 0, 0]);
        let lambda = (three_x2 + a_weierstrass()) * (y_affine.double()).inverse_or_zero();

        let x_new = lambda.square() - x_affine.double();
        let y_new = lambda * (x_affine - x_new) - y_affine;

        Self { x: x_new, y: y_new, z: GFp5::ONE }
    }

    /// Point addition
    pub fn add(&self, other: &Self) -> Self {
        if self.is_infinity() {
            return *other;
        }
        if other.is_infinity() {
            return *self;
        }

        let x1 = self.x * self.z.inverse_or_zero();
        let y1 = self.y * self.z.inverse_or_zero();
        let x2 = other.x * other.z.inverse_or_zero();
        let y2 = other.y * other.z.inverse_or_zero();

        if x1 == x2 {
            if y1 == y2 {
                return self.double();
            } else {
                return Self::infinity();
            }
        }

        let lambda = (y2 - y1) * (x2 - x1).inverse_or_zero();
        let x3 = lambda.square() - x1 - x2;
        let y3 = lambda * (x1 - x3) - y1;

        Self { x: x3, y: y3, z: GFp5::ONE }
    }

    /// Scalar multiplication
    pub fn mul(&self, scalar: &ECgFp5Scalar) -> Self {
        if scalar.is_zero() {
            return Self::infinity();
        }

        let mut result = Self::infinity();
        let bits = scalar_to_bits(scalar);

        for bit in bits.iter().rev() {
            result = result.double();
            if *bit {
                result = result.add(self);
            }
        }

        result
    }

    /// Combined scalar multiplication: s*G + e*pk
    pub fn mul_add2(g: &Self, pk: &Self, s: &ECgFp5Scalar, e: &ECgFp5Scalar) -> Self {
        let s_g = g.mul(s);
        let e_pk = pk.mul(e);
        s_g.add(&e_pk)
    }
}

/// Convert scalar to bits (little-endian)
fn scalar_to_bits(scalar: &ECgFp5Scalar) -> Vec<bool> {
    let mut bits = Vec::with_capacity(320);
    for limb in scalar.0.iter() {
        for i in 0..64 {
            bits.push((limb >> i) & 1 == 1);
        }
    }
    bits
}

impl ECgFp5Point {
    /// Neutral (identity) point
    pub fn neutral() -> Self {
        Self {
            x: GFp5::ZERO,
            z: GFp5::ONE,
            u: GFp5::ZERO,
            t: GFp5::ONE,
        }
    }

    /// Generator point
    pub fn generator() -> Self {
        Self {
            x: GFp5::from_u64_array([
                12883135586176881569,
                4356519642755055268,
                5248930565894896907,
                2165973894480315022,
                2448410071095648785,
            ]),
            z: GFp5::ONE,
            u: GFp5::ONE,
            t: GFp5::from_u64_array([4, 0, 0, 0, 0]),
        }
    }

    /// Check if this is the neutral point
    pub fn is_neutral(&self) -> bool {
        self.u.is_zero()
    }

    /// Encode point to GFp5 element (w = y/x = t/u)
    pub fn encode(&self) -> GFp5 {
        self.t * self.u.inverse_or_zero()
    }

    /// Decode GFp5 element to point
    pub fn decode(w: GFp5) -> Option<Self> {
        let e = w.square() - A;
        let delta = e.square() - b_mul4();

        let r = delta.canonical_sqrt();
        let c = r.is_some();

        let r = r.unwrap_or(GFp5::ZERO);

        let x1 = (e + r).div(GFp5::TWO);
        let x2 = (e - r).div(GFp5::TWO);

        let x1_legendre = x1.legendre();
        let x = if x1_legendre.to_u64() == 1 { x1 } else { x2 };

        let (x, u, t) = if !c {
            (GFp5::ZERO, GFp5::ZERO, GFp5::ONE)
        } else {
            (x, GFp5::ONE, w)
        };

        if c || w.is_zero() {
            Some(Self { x, z: GFp5::ONE, u, t })
        } else {
            None
        }
    }

    /// Point addition
    pub fn add(&self, rhs: &Self) -> Self {
        let x1 = self.x;
        let z1 = self.z;
        let u1 = self.u;
        let t1 = self.t;

        let x2 = rhs.x;
        let z2 = rhs.z;
        let u2 = rhs.u;
        let t2 = rhs.t;

        let t1_val = x1 * x2;
        let t2_val = z1 * z2;
        let t3 = u1 * u2;
        let t4 = t1 * t2;
        let t5 = (x1 + z1) * (x2 + z2) - t1_val - t2_val;
        let t6 = (u1 + t1) * (u2 + t2) - t3 - t4;
        let t7 = t1_val + t2_val * b();
        let t8 = t4 * t7;
        let t9 = t3 * (t5 * b_mul2() + t7.double());
        let t10 = (t4 + t3.double()) * (t5 + t7);

        let x_new = (t10 - t8) * b();
        let z_new = t8 - t9;
        let u_new = t6 * (t2_val * b() - t1_val);
        let t_new = t8 + t9;

        Self { x: x_new, z: z_new, u: u_new, t: t_new }
    }

    /// Point doubling
    pub fn double(&self) -> Self {
        let x = self.x;
        let z = self.z;
        let u = self.u;
        let t = self.t;

        let t1 = z * t;
        let t2 = t1 * t;
        let x1 = t2.square();
        let z1 = t1 * u;
        let t3 = u.square();
        let w1 = t2 - t3 * (x + z).double();
        let t4 = z1.square();

        let x_new = t4 * b_mul4();
        let z_new = w1.square();
        let u_new = (w1 + z1).square() - t4 - z_new;
        let t_new = x1.double() - (t4 * GFp5::from_u64_array([4, 0, 0, 0, 0]) + z_new);

        Self { x: x_new, z: z_new, u: u_new, t: t_new }
    }

    /// Multiple doubling
    pub fn mdouble(&self, n: u32) -> Self {
        if n == 0 {
            return *self;
        }

        let mut result = *self;
        for _ in 0..n {
            result = result.double();
        }
        result
    }

    /// Scalar multiplication
    pub fn mul(&self, scalar: &ECgFp5Scalar) -> Self {
        const WINDOW: usize = 5;
        const WIN_SIZE: usize = 1 << (WINDOW - 1);

        let mut win = [*self; WIN_SIZE];
        for i in 1..WIN_SIZE {
            if i & 1 == 0 {
                win[i] = win[i - 1].add(self);
            } else {
                win[i] = win[i >> 1].double();
            }
        }

        let mut digits = vec![0i32; (319 + WINDOW) / WINDOW];
        scalar.recode_signed(&mut digits, WINDOW as i32);

        let mut result = Self::lookup_vartime(&win, digits[digits.len() - 1]);
        for i in (0..digits.len() - 1).rev() {
            result = result.mdouble(WINDOW as u32);
            let lookup = Self::lookup(&win, digits[i]);
            result = result.add(&lookup);
        }

        result
    }

    fn lookup(table: &[Self], digit: i32) -> Self {
        let sign = (digit >> 31) as u64;
        let abs_digit = ((digit ^ (digit >> 31)) - (digit >> 31)) as usize;

        let mut result = Self::neutral();
        for (i, p) in table.iter().enumerate() {
            let select = if i + 1 == abs_digit { u64::MAX } else { 0 };
            result = Self::select(select, &result, p);
        }

        if sign != 0 {
            result = Self::negate(&result);
        }

        result
    }

    fn lookup_vartime(table: &[Self], digit: i32) -> Self {
        if digit == 0 {
            return Self::neutral();
        }

        let abs_digit = digit.unsigned_abs() as usize;
        let p = if abs_digit <= table.len() {
            table[abs_digit - 1]
        } else {
            Self::neutral()
        };

        if digit < 0 {
            Self::negate(&p)
        } else {
            p
        }
    }

    fn select(mask: u64, a: &Self, b: &Self) -> Self {
        Self {
            x: Self::select_gfp5(mask, a.x, b.x),
            z: Self::select_gfp5(mask, a.z, b.z),
            u: Self::select_gfp5(mask, a.u, b.u),
            t: Self::select_gfp5(mask, a.t, b.t),
        }
    }

    fn select_gfp5(mask: u64, a: GFp5, b: GFp5) -> GFp5 {
        GFp5([
            Self::select_field(mask, a.0[0], b.0[0]),
            Self::select_field(mask, a.0[1], b.0[1]),
            Self::select_field(mask, a.0[2], b.0[2]),
            Self::select_field(mask, a.0[3], b.0[3]),
            Self::select_field(mask, a.0[4], b.0[4]),
        ])
    }

    fn select_field(mask: u64, a: GoldilocksField, b: GoldilocksField) -> GoldilocksField {
        GoldilocksField(a.0 ^ (mask & (a.0 ^ b.0)))
    }

    fn negate(p: &Self) -> Self {
        Self {
            x: p.x,
            z: p.z,
            u: -p.u,
            t: p.t,
        }
    }
}

/// Scalar field for ECgFp5
/// Order n is approximately 2^319
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ECgFp5Scalar(pub [u64; 5]);

/// Group order as BigUint
fn order() -> BigUint {
    BigUint::parse_bytes(
        b"1067993516717146951041484916571792702745057740581727230159139685185762082554198619328292418486241",
        10
    ).unwrap()
}

/// N modulus for Montgomery reduction
const N: [u64; 5] = [
    0xE80FD996948BFFE1,
    0xE8885C39D724A09C,
    0x7FFFFFE6CFB80639,
    0x7FFFFFF100000016,
    0x7FFFFFFD80000007,
];

/// -1/N[0] mod 2^64
const N0I: u64 = 0xD78BEF72057B7BDF;

/// R^2 mod N (for Montgomery multiplication)
const R2: [u64; 5] = [
    0xA01001DCE33DC739,
    0x6C3228D33F62ACCF,
    0xD1D796CC91CF8525,
    0xAADFFF5D1574C1D8,
    0x4ACA13B28CA251F5,
];

impl ECgFp5Scalar {
    pub const ZERO: Self = Self([0; 5]);
    pub const ONE: Self = Self([1, 0, 0, 0, 0]);

    /// Create from 5 u64 limbs
    pub fn from_limbs(limbs: [u64; 5]) -> Self {
        Self(limbs)
    }

    /// Get the 5 u64 limbs
    pub fn to_limbs(&self) -> [u64; 5] {
        self.0
    }

    /// Create from little-endian bytes (40 bytes)
    pub fn from_le_bytes(data: &[u8]) -> Self {
        assert!(data.len() == 40, "ECgFp5Scalar requires 40 bytes");

        let mut value = [0u64; 5];
        for i in 0..5 {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&data[i * 8..(i + 1) * 8]);
            value[i] = u64::from_le_bytes(bytes);
        }
        Self(value)
    }

    /// Convert to little-endian bytes
    pub fn to_le_bytes(&self) -> [u8; 40] {
        let mut result = [0u8; 40];
        for i in 0..5 {
            result[i * 8..(i + 1) * 8].copy_from_slice(&self.0[i].to_le_bytes());
        }
        result
    }

    /// Create from GFp5 element (reduce modulo group order)
    pub fn from_gfp5(elem: GFp5) -> Self {
        let arr = elem.to_u64_array();
        let big = bigint_from_array(&arr);
        let reduced = big % order();
        from_bigint(&reduced)
    }

    /// Check if zero
    pub fn is_zero(&self) -> bool {
        self.0.iter().all(|&x| x == 0)
    }

    /// Add two scalars
    pub fn add(&self, rhs: &Self) -> Self {
        let mut r = add_inner(&self.0, &rhs.0);
        let (r2, borrow) = sub_inner(&r, &N);
        if borrow == 0 {
            r = r2;
        }
        Self(r)
    }

    /// Subtract two scalars
    pub fn sub(&self, rhs: &Self) -> Self {
        let (r0, borrow) = sub_inner(&self.0, &rhs.0);
        let r = if borrow != 0 {
            add_inner(&r0, &N)
        } else {
            r0
        };
        Self(r)
    }

    /// Multiply two scalars using Montgomery multiplication
    pub fn mul(&self, rhs: &Self) -> Self {
        let tmp = monty_mul(&self.0, &R2);
        Self(monty_mul(&tmp, &rhs.0))
    }

    /// Recode scalar to signed digits for scalar multiplication
    pub fn recode_signed(&self, ss: &mut [i32], w: i32) {
        let mw = ((1u32 << w) - 1) as u32;
        let hw = (1u32 << (w - 1)) as u32;

        let mut acc: u64 = 0;
        let mut acc_len: i32 = 0;
        let mut j = 0usize;
        let mut cc: u32 = 0;

        for i in 0..ss.len() {
            let bb = if acc_len < w {
                if j < 5 {
                    let nl = self.0[j];
                    j += 1;
                    let tmp = ((acc | (nl << acc_len as u64)) as u32) & mw;
                    acc = nl >> (w - acc_len);
                    acc_len += 64 - w;
                    tmp
                } else {
                    let tmp = (acc as u32) & mw;
                    acc = 0;
                    acc_len += 64 - w;
                    tmp
                }
            } else {
                let tmp = (acc as u32) & mw;
                acc_len -= w;
                acc >>= w;
                tmp
            };

            let bb = bb + cc;
            cc = (hw.wrapping_sub(bb)) >> 31;
            ss[i] = (bb as i32) - ((cc << w) as i32);
        }
    }

    /// Sample random scalar using crypto RNG
    pub fn sample_random() -> Self {
        use rand::RngCore;
        let mut rng = rand::thread_rng();

        loop {
            let mut bytes = [0u8; 40];
            rng.fill_bytes(&mut bytes);

            // Clear top bit to ensure we're below 2^319
            bytes[39] &= 0x7F;

            let scalar = Self::from_le_bytes(&bytes);

            // Check if below group order
            let val = bigint_from_array(&scalar.0);
            if val < order() {
                return scalar;
            }
        }
    }
}

// Helper functions for scalar arithmetic

fn add_inner(a: &[u64; 5], b: &[u64; 5]) -> [u64; 5] {
    let mut r = [0u64; 5];
    let mut carry = 0u64;
    for i in 0..5 {
        let (sum, c1) = a[i].overflowing_add(b[i]);
        let (sum, c2) = sum.overflowing_add(carry);
        r[i] = sum;
        carry = (c1 as u64) + (c2 as u64);
    }
    r
}

fn sub_inner(a: &[u64; 5], b: &[u64; 5]) -> ([u64; 5], u64) {
    let mut r = [0u64; 5];
    let mut borrow = 0u64;
    for i in 0..5 {
        let (diff, b1) = a[i].overflowing_sub(b[i]);
        let (diff, b2) = diff.overflowing_sub(borrow);
        r[i] = diff;
        borrow = (b1 as u64) + (b2 as u64);
    }
    (r, if borrow != 0 { u64::MAX } else { 0 })
}

/// Montgomery multiplication
fn monty_mul(a: &[u64; 5], b: &[u64; 5]) -> [u64; 5] {
    let mut r = [0u64; 5];

    for i in 0..5 {
        let m = b[i];
        let f = (a[0].wrapping_mul(m).wrapping_add(r[0])).wrapping_mul(N0I);

        let mut cc1 = 0u128;
        let mut cc2 = 0u128;

        for j in 0..5 {
            let z1 = (a[j] as u128) * (m as u128) + (r[j] as u128) + cc1;
            cc1 = z1 >> 64;
            let z2 = (f as u128) * (N[j] as u128) + (z1 as u64 as u128) + cc2;
            cc2 = z2 >> 64;
            if j > 0 {
                r[j - 1] = z2 as u64;
            }
        }
        r[4] = (cc1 + cc2) as u64;
    }

    // Final reduction
    let (r2, borrow) = sub_inner(&r, &N);
    if borrow == 0 { r2 } else { r }
}

fn bigint_from_array(arr: &[u64; 5]) -> BigUint {
    let mut result = BigUint::zero();
    for &limb in arr.iter().rev() {
        result <<= 64;
        result += BigUint::from(limb);
    }
    result
}

fn from_bigint(val: &BigUint) -> ECgFp5Scalar {
    let bytes = val.to_bytes_le();
    let mut arr = [0u8; 40];
    let len = bytes.len().min(40);
    arr[..len].copy_from_slice(&bytes[..len]);
    ECgFp5Scalar::from_le_bytes(&arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator() {
        let g = ECgFp5Point::generator();
        assert!(!g.is_neutral());
    }

    #[test]
    fn test_neutral() {
        let n = ECgFp5Point::neutral();
        assert!(n.is_neutral());
    }

    #[test]
    fn test_scalar_mul() {
        let g = ECgFp5Point::generator();
        let scalar = ECgFp5Scalar::ONE;
        let result = g.mul(&scalar);
        let g_encoded = g.encode();
        let result_encoded = result.encode();
        assert_eq!(g_encoded.to_u64_array(), result_encoded.to_u64_array());
    }
}
