use std::thread;

use super::*;

#[test]
fn test_log_completion_starts_at_last_log_id() {
  let completion = LogCompletion::new(5);
  assert_eq!(completion.get_frontier(), 5);
}

#[test]
fn test_log_completion_in_order() {
  let completion = LogCompletion::new(0);
  for id in 0..100 {
    completion.complete(id);
    assert_eq!(completion.get_frontier(), id + 1);
  }
}

#[test]
fn test_log_completion_out_of_order() {
  let completion = LogCompletion::new(0);
  for id in (1..100).rev() {
    completion.complete(id);
    assert_eq!(completion.get_frontier(), 0);
  }

  completion.complete(0);
  assert_eq!(completion.get_frontier(), 100);
}

#[test]
fn test_log_completion_stops_at_gap() {
  let completion = LogCompletion::new(10);
  completion.complete(10);
  completion.complete(12);
  completion.complete(13);
  assert_eq!(completion.get_frontier(), 11);

  completion.complete(11);
  assert_eq!(completion.get_frontier(), 14);

  completion.complete(15);
  assert_eq!(completion.get_frontier(), 14);
}

#[test]
fn test_log_completion_concurrent() {
  const THREADS: u64 = 8;
  const COUNT: u64 = 10_000;

  let completion = LogCompletion::new(0);
  thread::scope(|s| {
    for t in 0..THREADS {
      let completion = &completion;
      s.spawn(move || {
        for id in (t..COUNT).step_by(THREADS as usize) {
          completion.complete(id);
        }
      });
    }
  });

  assert_eq!(completion.get_frontier(), COUNT);
}
