use std::time::Duration;

use super::*;

#[test]
fn test_empty_snapshot() {
  let snapshot = Histogram::new(1).snapshot();
  assert_eq!(snapshot.total_count(), 0);
  assert_eq!(snapshot.average(), 0.0);
  assert_eq!(snapshot.percentile(0.99), 0.0);
}

#[test]
fn test_bucket_boundary() {
  let histogram = Histogram::new(1);
  histogram.record(0.1);
  let p = histogram.snapshot().percentile(1.0);
  assert!((0.0..=0.1).contains(&p), "p {p}");

  let histogram = Histogram::new(1);
  histogram.record(0.15);
  let p = histogram.snapshot().percentile(1.0);
  assert!(p > 0.1 && p <= 0.2, "p {p}");
}

#[test]
fn test_average() {
  let histogram = Histogram::new(1);
  for value in [1.0, 2.0, 3.0, 6.0] {
    histogram.record(value);
  }

  let snapshot = histogram.snapshot();
  assert_eq!(snapshot.total_count(), 4);
  assert_eq!(snapshot.average(), 3.0);
}

#[test]
fn test_sampling() {
  let histogram = Histogram::new(10);
  for i in 0..101 {
    histogram.record(if i % 10 == 0 { 1.0 } else { 1000.0 });
  }

  let snapshot = histogram.snapshot();
  assert_eq!(snapshot.total_count(), 101);
  assert_eq!(snapshot.average(), 1.0);
}

#[test]
fn test_percentile() {
  let histogram = Histogram::new(1);
  for _ in 0..100 {
    histogram.record(3.0);
  }
  for _ in 0..100 {
    histogram.record(300.0);
  }

  let snapshot = histogram.snapshot();
  let p25 = snapshot.percentile(0.25);
  let p75 = snapshot.percentile(0.75);
  assert!((2.0..=5.0).contains(&p25), "p25 {p25}");
  assert!((200.0..=500.0).contains(&p75), "p75 {p75}");
}

#[test]
fn test_percentile_overflow_bucket() {
  let histogram = Histogram::new(1);
  histogram.record(20_000_000.0);
  assert_eq!(histogram.snapshot().percentile(0.5), 10_000_000.0);
}

#[test]
fn test_time_histogram_sampling() {
  let histogram = TimeHistogram::new(2, Duration::from_millis(1));
  let first = histogram.start();
  let second = histogram.start();
  let third = histogram.start();
  assert!(first.is_some());
  assert!(second.is_none());
  assert!(third.is_some());

  histogram.record(first);
  histogram.record(second);
  histogram.record(third);
  assert_eq!(
    histogram
      .snapshot_with(Duration::from_millis(1))
      .total_count(),
    3
  );
}

#[test]
fn test_time_histogram_unit_conversion() {
  let histogram = TimeHistogram::new(1, Duration::from_millis(1));
  for _ in 0..10 {
    let start = histogram.start();
    histogram.record(start);
  }

  let millis = histogram.snapshot_with(Duration::from_millis(1)).average();
  let micros = histogram.snapshot_with(Duration::from_micros(1)).average();
  assert!((micros - millis * 1_000.0).abs() <= micros.abs() * 1e-9);
}
