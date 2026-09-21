use super::*;

#[test]
fn test_gauge() {
  let gauge = Gauge::new();
  let c = 10;
  for _ in 0..c {
    gauge.inc();
  }
  assert_eq!(gauge.load(), c);

  let d = 7;
  for _ in 0..d {
    gauge.dec();
  }
  assert_eq!(gauge.load(), c - d);
}
