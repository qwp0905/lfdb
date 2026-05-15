use super::*;

#[test]
fn test_registry() {
  let registry = MetricsRegistry::new();
  let snapshot = registry.snapshot();

  assert_eq!(snapshot.active_io_threads, 0)
}
