//! Cryptographic primitives for Lighter
//!
//! This module provides pure Rust implementations of:
//! - Goldilocks field (64-bit prime field)
//! - GFp5 (quintic extension of Goldilocks)
//! - Poseidon2 hash function
//! - ECgFp5 elliptic curve
//! - Schnorr signatures

pub mod goldilocks;
pub mod gfp5;
pub mod poseidon2;
pub mod ecgfp5;
pub mod schnorr;

pub use goldilocks::GoldilocksField;
pub use gfp5::GFp5;
pub use ecgfp5::{ECgFp5Point, ECgFp5Scalar};
pub use schnorr::{Signature, schnorr_pk_from_sk, schnorr_sign_hashed_message, verify_signature};
