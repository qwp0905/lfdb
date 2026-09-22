use super::*;

#[test]
fn test_counter() {
  let counter = Counter::new();
  let c = 10;
  for _ in 0..c {
    counter.inc();
  }
}
