use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Barrier,
  },
  thread,
};

use crate::utils::MpscQueue;

#[test]
fn test_push_and_pop() {
  for count in [0, 1, 30, 31, 32, 62, 63, 1024] {
    let queue = MpscQueue::new();

    assert!(queue.is_empty());
    assert_eq!(unsafe { queue.pop() }, None);

    for i in 0..count {
      queue.push(i);
    }

    assert_eq!(queue.is_empty(), count == 0);
    for i in 0..count {
      assert_eq!(unsafe { queue.pop() }, Some(i));
    }

    assert!(queue.is_empty());
    for _ in 0..3 {
      assert_eq!(unsafe { queue.pop() }, None);
    }
  }
}

#[test]
fn test_push_after_empty() {
  let queue = MpscQueue::new();
  for i in 0..1024 {
    queue.push(i);

    // SAFETY: this test is the only consumer.
    assert!(!queue.is_empty());
    assert_eq!(unsafe { queue.pop() }, Some(i));
    assert!(queue.is_empty());
    assert_eq!(unsafe { queue.pop() }, None);
  }
}

#[test]
fn test_multiple_producers() {
  const PRODUCERS: usize = 4;
  const COUNT: usize = 1024;

  let queue = MpscQueue::new();
  let barrier = Barrier::new(PRODUCERS + 1);
  thread::scope(|scope| {
    for producer in 0..PRODUCERS {
      let queue = &queue;
      let barrier = &barrier;
      scope.spawn(move || {
        barrier.wait();
        for i in 0..COUNT {
          queue.push((producer, i));
        }
      });
    }

    barrier.wait();
    let mut expected = [0; PRODUCERS];
    for _ in 0..PRODUCERS * COUNT {
      let (producer, i) = loop {
        // SAFETY: producer threads only push; this thread is the only consumer.
        if let Some(value) = unsafe { queue.pop() } {
          break value;
        }
        thread::yield_now();
      };
      assert_eq!(i, expected[producer]);
      expected[producer] += 1;
    }

    assert_eq!(expected, [COUNT; PRODUCERS]);
    assert!(queue.is_empty());
    assert_eq!(unsafe { queue.pop() }, None);
  });
}

#[test]
fn test_drop() {
  struct DC<'a>(&'a AtomicUsize);
  impl Drop for DC<'_> {
    fn drop(&mut self) {
      self.0.fetch_add(1, Ordering::Relaxed);
    }
  }

  for (push_count, pop_count) in [
    (0, 0),
    (1, 0),
    (1, 1),
    (31, 0),
    (31, 30),
    (31, 31),
    (32, 1),
    (32, 31),
    (32, 32),
    (1024, 129),
  ] {
    let counter = AtomicUsize::new(0);
    let queue = MpscQueue::new();
    for _ in 0..push_count {
      queue.push(DC(&counter));
    }

    for _ in 0..pop_count {
      // SAFETY: this test is the only consumer.
      drop(unsafe { queue.pop() }.unwrap());
    }
    assert_eq!(counter.load(Ordering::Relaxed), pop_count);

    drop(queue);
    assert_eq!(counter.load(Ordering::Relaxed), push_count);
  }
}
