use super::super::ThreadBuilder;
use std::time::Duration;

#[test]
fn test_multiple_close() {
  let thread = ThreadBuilder::new()
    .single()
    .interval(Duration::from_secs(10), || {});

  thread.close();
  thread.close();
}
