use std::{
  array,
  sync::atomic::{AtomicU64, Ordering},
  time::{Duration, Instant},
};

use crossbeam::utils::Backoff;

const BUCKET_SIZE: usize = 25;

const BUCKET_BOUND: [f64; BUCKET_SIZE] = [
  0.1,
  0.2,
  0.5,
  1.0,
  2.0,
  5.0,
  10.0,
  20.0,
  50.0,
  100.0,
  200.0,
  500.0,
  1_000.0,
  2_000.0,
  5_000.0,
  10_000.0,
  20_000.0,
  50_000.0,
  100_000.0,
  200_000.0,
  500_000.0,
  1_000_000.0,
  2_000_000.0,
  5_000_000.0,
  10_000_000.0,
];

struct AtomicF64(AtomicU64);
impl AtomicF64 {
  const fn new(value: f64) -> Self {
    Self(AtomicU64::new(value.to_bits()))
  }
  fn add(&self, value: f64) {
    let backoff = Backoff::new();
    let mut old = self.0.load(Ordering::Acquire);
    loop {
      let new = f64::from_bits(old) + value;
      let Err(err) = self.0.compare_exchange_weak(
        old,
        new.to_bits(),
        Ordering::Relaxed,
        Ordering::Relaxed,
      ) else {
        return;
      };

      old = err;
      backoff.snooze();
    }
  }
  fn load(&self) -> f64 {
    f64::from_bits(self.0.load(Ordering::Acquire))
  }
}

pub struct TimeHistogram {
  histogram: Histogram,
  unit: Duration,
}
impl TimeHistogram {
  pub fn new(sample: u64, unit: Duration) -> Self {
    Self {
      histogram: Histogram::new(sample),
      unit,
    }
  }
  pub fn snapshot_with(&self, present_unit: Duration) -> TimeHistogramSnapshot {
    let factor = self.unit.div_duration_f64(present_unit);
    TimeHistogramSnapshot::new(self.histogram.snapshot(), factor)
  }

  pub fn start(&self) -> Option<Instant> {
    if !self.histogram.sample() {
      return None;
    }
    Some(Instant::now())
  }
  pub fn record(&self, start: Option<Instant>) {
    let Some(start) = start else { return };
    let elapsed = start.elapsed().div_duration_f64(self.unit);
    self.histogram.apply_value(elapsed);
  }
}
pub struct Histogram {
  count: AtomicU64,
  buckets: [AtomicU64; BUCKET_SIZE + 1],
  sum: AtomicF64,
  sample: u64,
}
impl Histogram {
  pub fn new(sample: u64) -> Self {
    Self {
      count: AtomicU64::new(0),
      buckets: array::from_fn(|_| AtomicU64::new(0)),
      sum: AtomicF64::new(0.0),
      sample,
    }
  }

  pub fn snapshot(&self) -> HistogramSnapshot {
    let total = self.count.load(Ordering::Relaxed);
    HistogramSnapshot {
      total_count: total,
      sample_count: total.div_ceil(self.sample),
      buckets: array::from_fn(|i| self.buckets[i].load(Ordering::Relaxed)),
      sum: self.sum.load(),
    }
  }

  fn apply_value(&self, value: f64) {
    self.sum.add(value);
    let i = BUCKET_BOUND.partition_point(|&b| value > b);
    self.buckets[i].fetch_add(1, Ordering::Relaxed);
  }
  fn sample(&self) -> bool {
    let n = self.count.fetch_add(1, Ordering::Relaxed);
    n % self.sample == 0
  }

  pub fn record(&self, value: f64) {
    if !self.sample() {
      return;
    }
    self.apply_value(value);
  }
}

pub struct TimeHistogramSnapshot {
  snapshot: HistogramSnapshot,
  factor: f64,
}
impl TimeHistogramSnapshot {
  const fn new(snapshot: HistogramSnapshot, factor: f64) -> Self {
    Self { snapshot, factor }
  }
  pub const fn total_count(&self) -> u64 {
    self.snapshot.total_count()
  }
  pub const fn average(&self) -> f64 {
    self.snapshot.average() * self.factor
  }
  pub fn percentile(&self, q: f64) -> f64 {
    self.snapshot.percentile(q) * self.factor
  }
}

#[derive(Debug)]
pub struct HistogramSnapshot {
  sample_count: u64,
  total_count: u64,
  buckets: [u64; BUCKET_SIZE + 1],
  sum: f64,
}
impl HistogramSnapshot {
  #[inline]
  pub const fn total_count(&self) -> u64 {
    self.total_count
  }
  pub const fn average(&self) -> f64 {
    if self.sample_count == 0 {
      return 0.0;
    }

    self.sum / self.sample_count as f64
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
        let lower = if i == 0 { 0.0 } else { BUCKET_BOUND[i - 1] };
        let upper = if i < BUCKET_BOUND.len() {
          BUCKET_BOUND[i]
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
    BUCKET_BOUND.last().copied().unwrap_or(0.0)
  }
}

#[macro_export]
macro_rules! measure {
  ($metrics:expr, $block:expr $(,)?) => {{
    let start = $metrics.start();
    let result = $block;
    $metrics.record(start);
    result
  }};
}
