use std::sync::atomic::AtomicU32;

use crate::{disk::POINTER_BYTES, objects::SERIALIZABLE_BYTES, wal::TX_ID_BYTES};

pub const MAX_KEY: usize = 1 << 8;
pub const MAX_VALUE: usize = 32 << 20;

/**
 * Maximum inline value size that still lets a leaf node hold at least two inline
 * value entries. Larger values must be stored as blobs instead of occupying leaf
 * payload directly.
 */
pub const LARGE_VALUE: usize =
  ((SERIALIZABLE_BYTES - (1 + POINTER_BYTES + 2 + SPLIT_BIAS_BYTES)) >> 1)
    - (MAX_KEY + POINTER_BYTES + 2 + (TX_ID_BYTES << 1) + 1 + 2);

pub type RecordId = u32;
pub const RECORD_ID_BYTES: usize = RecordId::BITS as usize >> 3;
pub type AtomicRecordId = AtomicU32;

/**
 * Bit-packed split-bias history.
 *
 * Each sample records whether a recent insertion landed in the left, middle, or
 * right part of the node. Two bits are used per sample, and the default history
 * is filled with middle samples so a node starts with a neutral split bias.
 */
pub type SplitBias = u32;
pub const SPLIT_BIAS_BYTES: usize = SplitBias::BITS as usize >> 3;

pub type StaticKey = Vec<u8>;
pub type StaticKeyRef<'a> = &'a [u8];
