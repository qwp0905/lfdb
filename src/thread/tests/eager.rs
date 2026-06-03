use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const DEFAULT_STACK_SIZE: usize = 64 << 10;

#[test]
fn test_basic_send_and_receive() {
  let thread = EagerBufferingThread::new(
    "test-basic",
    DEFAULT_STACK_SIZE,
    16,
    SingleFn::new(|values: Vec<usize>| values.iter().sum::<usize>()),
  );

  let result = thread.execute(10).wait().unwrap();
  assert_eq!(result, 10);

  let result = thread.execute(20).wait().unwrap();
  assert_eq!(result, 20);

  thread.close();
}

#[test]
fn test_try_recv_drains_pending() {
  let batch_sizes = Arc::new(Mutex::new(vec![]));
  let batch_sizes_c = batch_sizes.clone();

  let thread = EagerBufferingThread::new(
    "test-drain",
    DEFAULT_STACK_SIZE,
    64,
    SingleFn::new(move |values: Vec<usize>| {
      batch_sizes_c.lock().unwrap().push(values.len());
      // slow processing so requests accumulate
      thread::sleep(Duration::from_millis(50));
      0usize
    }),
  );

  // trigger first recv (blocking) and wait
  thread.execute(1).wait().unwrap();

  // now worker is back to blocking recv
  // send many rapidly — they should be batched by try_recv
  let pending: Vec<_> = (0..20).map(|i| thread.execute(i)).collect();
  for r in pending {
    let _ = r.wait().unwrap();
  }

  {
    let sizes = batch_sizes.lock().unwrap();
    // at least one batch should have > 1 items
    assert!(
      sizes.iter().any(|&s| s > 1),
      "expected at least one batch > 1, got {:?}",
      *sizes
    );
  }

  thread.close();
}

#[test]
fn test_max_count_respected() {
  let batch_sizes = Arc::new(Mutex::new(vec![]));
  let batch_sizes_c = batch_sizes.clone();
  let max_count = 5;

  let thread = EagerBufferingThread::new(
    "test-max-count",
    DEFAULT_STACK_SIZE,
    max_count,
    SingleFn::new(move |values: Vec<usize>| {
      let len = values.len();
      batch_sizes_c.lock().unwrap().push(len);
      len
    }),
  );

  // send more than max_count
  let pending: Vec<_> = (0..20).map(|i| thread.execute(i)).collect();
  for r in pending {
    let _ = r.wait().unwrap();
  }

  {
    let sizes = batch_sizes.lock().unwrap();
    // no batch should exceed max_count
    for &size in sizes.iter() {
      assert!(
        size <= max_count,
        "batch size {size} exceeded max_count {max_count}"
      );
    }
  }

  thread.close();
}

#[test]
fn test_result_per_item() {
  let thread = EagerBufferingThread::new(
    "test-result",
    DEFAULT_STACK_SIZE,
    16,
    SingleFn::new(|values: Vec<usize>| values.iter().map(|v| v * 3).collect::<Vec<_>>()),
  );

  // each sender should get the shared result
  let r1 = thread.execute(5);
  let result = r1.wait().unwrap();
  assert_eq!(result, vec![15]);

  thread.close();
}

#[test]
fn test_close_flushes_remaining() {
  let total = Arc::new(AtomicUsize::new(0));
  let total_c = total.clone();

  let thread = EagerBufferingThread::new(
    "test-close-flush",
    DEFAULT_STACK_SIZE,
    1024,
    SingleFn::new(move |values: Vec<usize>| {
      total_c.fetch_add(values.len(), Ordering::Release);
      values.len()
    }),
  );

  // send items
  for i in 0..5 {
    let _ = thread.execute(i);
  }

  // close should flush remaining
  thread.close();

  assert_eq!(total.load(Ordering::Acquire), 5);
}

#[test]
fn test_multiple_close() {
  let thread = EagerBufferingThread::new(
    "test-multi-close",
    DEFAULT_STACK_SIZE,
    16,
    SingleFn::new(|v: Vec<()>| v.len()),
  );

  thread.close();
  thread.close();
  thread.close();
}

#[test]
fn test_panic_recovery() {
  let thread = EagerBufferingThread::new(
    "test-panic",
    DEFAULT_STACK_SIZE,
    16,
    SingleFn::new(|_: Vec<usize>| -> usize { panic!("intentional panic") }),
  );

  let result = thread.execute(1).wait();
  assert!(result.is_err());

  thread.close();
}

#[test]
fn test_concurrent_senders() {
  let total = Arc::new(AtomicUsize::new(0));
  let total_c = total.clone();

  let thread = Arc::new(EagerBufferingThread::new(
    "test-concurrent",
    DEFAULT_STACK_SIZE,
    64,
    SingleFn::new(move |values: Vec<usize>| {
      let sum: usize = values.iter().sum();
      total_c.fetch_add(sum, Ordering::Release);
      sum
    }),
  ));

  let mut handles = vec![];
  for i in 0..4 {
    let t = thread.clone();
    handles.push(thread::spawn(move || {
      for j in 0..25 {
        let _ = t.execute(i * 25 + j).wait().unwrap();
      }
    }));
  }

  for h in handles {
    h.join().unwrap();
  }

  // sum of 0..100
  assert_eq!(total.load(Ordering::Acquire), (0..100).sum::<usize>());

  thread.close();
}
