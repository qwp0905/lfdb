use super::SplitBias;

const DIRECTIONS: usize = 3;

const CAP: u8 = SplitBias::BITS as u8 >> 1;
pub const DEFAULT_BIAS: SplitBias = {
  let mut b = 0;
  let mut c = 0;
  while c < CAP {
    b = b << 2 | 1;
    c += 1;
  }
  b
};

pub const fn update_bias(bias: SplitBias, len: usize, pos: usize) -> SplitBias {
  if len < DIRECTIONS {
    return bias;
  }
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
