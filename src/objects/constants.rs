pub const MAX_KEY: usize = 1 << 8;
pub const MAX_VALUE: usize = 32 << 20;

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
