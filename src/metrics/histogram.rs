use std::{
  array,
  sync::atomic::{AtomicU64, Ordering},
  time::{Duration, Instant},
};

const BUCKET_SIZE: usize = 20;

const BUCKET_BOUND: [u64; BUCKET_SIZE] = [
  1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000,
  100_000, 200_000, 500_000, 1_000_000, 2_000_000,
];
pub struct Histogram {
  count: AtomicU64,
  buckets: [AtomicU64; BUCKET_SIZE + 1],
  sum_elapse: AtomicU64,
  unit: u128,
  sample: u64,
}
impl Histogram {
  pub fn new(sample: u64, unit: Duration) -> Self {
    Self {
      count: AtomicU64::new(0),
      buckets: array::from_fn(|_| AtomicU64::new(0)),
      sum_elapse: AtomicU64::new(0),
      unit: unit.as_nanos(),
      sample,
    }
  }

  pub fn snapshot(&self) -> HistogramSnapshot {
    let total = self.count.load(Ordering::Relaxed);
    HistogramSnapshot {
      total_count: total,
      sample_count: total.div_ceil(self.sample),
      buckets: array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed)),
      sum_elapse: self.sum_elapse.load(Ordering::Relaxed),
    }
  }

  #[inline]
  pub fn start(&self) -> Option<Instant> {
    let n = self.count.fetch_add(1, Ordering::Relaxed);
    if n % self.sample != 0 {
      return None;
    }
    Some(Instant::now())
  }

  #[inline]
  pub fn measure<T, F>(&self, f: F) -> T
  where
    F: FnOnce() -> T,
  {
    let start = self.start();
    let result = f();
    self.record(start);
    result
  }

  #[inline]
  pub fn record(&self, start: Option<Instant>) {
    let elapsed = match start {
      Some(s) => (s.elapsed().as_nanos() / self.unit) as u64,
      None => return,
    };
    self.sum_elapse.fetch_add(elapsed, Ordering::Relaxed);
    let i = BUCKET_BOUND.partition_point(|&b| elapsed > b);
    self.buckets[i].fetch_add(1, Ordering::Relaxed);
  }
}

pub struct HistogramSnapshot {
  sample_count: u64,
  total_count: u64,
  buckets: [u64; BUCKET_SIZE + 1],
  sum_elapse: u64,
}
impl HistogramSnapshot {
  #[inline]
  pub fn total_count(&self) -> u64 {
    self.total_count
  }
  pub fn average(&self) -> f64 {
    if self.sample_count == 0 {
      return 0.0;
    }

    self.sum_elapse as f64 / self.sample_count as f64
  }

  pub fn percentile(&self, q: f64) -> f64 {
    if self.sample_count == 0 {
      return 0.0;
    }
    let target = self.sample_count as f64 * q;
    let mut cumulative = 0u64;

    for (i, &count) in self.buckets.iter().enumerate() {
      cumulative += count;
      if cumulative as f64 >= target {
        let lower = if i == 0 {
          0.0
        } else {
          BUCKET_BOUND[i - 1] as f64
        };
        let upper = if i < BUCKET_BOUND.len() {
          BUCKET_BOUND[i] as f64
        } else {
          lower
        };
        let count_below = (cumulative - count) as f64;
        let count_in = count as f64;
        if count_in == 0.0 {
          return lower;
        }
        return lower + (target - count_below) / count_in * (upper - lower);
      }
    }
    BUCKET_BOUND.last().copied().unwrap_or(0) as f64
  }
}
