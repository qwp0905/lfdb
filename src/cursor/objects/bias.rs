pub type SplitBias = u32;
pub const SPLIT_BIAS_BYTES: usize = SplitBias::BITS as usize >> 3;

const DIRECTIONS: usize = 3;

pub const DEFAULT_BIAS: SplitBias = 0b01010101_01010101_01010101_01010101;
const CAP: u8 = SplitBias::BITS as u8 >> 1;

pub const fn update_bias(bias: SplitBias, len: usize, pos: usize) -> SplitBias {
  let bucket = pos * DIRECTIONS / len;
  debug_assert!(bucket < DIRECTIONS);
  bias << 2 | bucket as SplitBias
}

pub fn count_directions(bias: SplitBias) -> [usize; DIRECTIONS] {
  let mut out = [0; DIRECTIONS];
  let mut v = bias;
  for _ in 0..CAP {
    out[(0b11 & v) as usize] += 1;
    v >>= 2;
  }
  out
}

#[cfg(test)]
#[path = "tests/bias.rs"]
mod tests;
