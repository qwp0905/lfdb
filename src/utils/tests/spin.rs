use std::sync::Arc;

use crate::utils::SpinRwLock;

#[test]
fn test_read_lock_basic() {
  let lock = SpinRwLock::new(42);
  let guard = lock.read();
  assert_eq!(*guard, 42);
}

#[test]
fn test_write_lock_basic() {
  let lock = SpinRwLock::new(0);
  {
    let mut guard = lock.write();
    *guard = 99;
  }
  let guard = lock.read();
  assert_eq!(*guard, 99);
}

#[test]
fn test_multiple_readers() {
  let lock = SpinRwLock::new(7);
  let r1 = lock.read();
  let r2 = lock.read();
  let r3 = lock.read();
  assert_eq!(*r1, 7);
  assert_eq!(*r2, 7);
  assert_eq!(*r3, 7);
}

#[test]
fn test_concurrent_readers() {
  let lock = Arc::new(SpinRwLock::new(100));
  let threads: Vec<_> = (0..10)
    .map(|_| {
      let l = lock.clone();
      std::thread::spawn(move || {
        for _ in 0..10_000 {
          let guard = l.read();
          assert!(*guard >= 100);
        }
      })
    })
    .collect();

  for t in threads {
    t.join().unwrap();
  }
}

#[test]
fn test_concurrent_writer_and_readers() {
  let lock = Arc::new(SpinRwLock::new(0u64));
  let iterations = 100_000;

  let writer = {
    let l = lock.clone();
    std::thread::spawn(move || {
      for i in 1..=iterations {
        let mut guard = l.write();
        *guard = i;
      }
    })
  };

  let readers: Vec<_> = (0..5)
    .map(|_| {
      let l = lock.clone();
      std::thread::spawn(move || {
        let mut last = 0;
        for _ in 0..iterations {
          let val = *l.read();
          assert!(val >= last, "value went backwards: {} -> {}", last, val);
          last = val;
        }
      })
    })
    .collect();

  writer.join().unwrap();
  for r in readers {
    r.join().unwrap();
  }

  assert_eq!(*lock.read(), iterations);
}

#[test]
fn test_concurrent_writers() {
  let lock = Arc::new(SpinRwLock::new(0u64));
  let per_thread = 10_000;
  let thread_count = 10;

  let threads: Vec<_> = (0..thread_count)
    .map(|_| {
      let l = lock.clone();
      std::thread::spawn(move || {
        for _ in 0..per_thread {
          let mut guard = l.write();
          *guard += 1;
        }
      })
    })
    .collect();

  for t in threads {
    t.join().unwrap();
  }

  assert_eq!(*lock.read(), per_thread * thread_count);
}

#[test]
fn test_drop_releases_write_lock() {
  let lock = SpinRwLock::new(1);
  {
    let mut guard = lock.write();
    *guard = 2;
    // guard dropped here
  }
  // should be able to acquire read lock
  let guard = lock.read();
  assert_eq!(*guard, 2);
}

#[test]
fn test_drop_releases_read_lock() {
  let lock = SpinRwLock::new(1);
  {
    let _r1 = lock.read();
    let _r2 = lock.read();
    // both dropped here
  }
  // should be able to acquire write lock
  let mut guard = lock.write();
  *guard = 3;
  assert_eq!(*guard, 3);
}

#[test]
fn test_default() {
  let lock: SpinRwLock<i32> = Default::default();
  assert_eq!(*lock.read(), 0);
}
