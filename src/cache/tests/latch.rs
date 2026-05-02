use std::{
  sync::atomic::{AtomicUsize, Ordering},
  thread::{scope, yield_now},
};

use crossbeam::queue::SegQueue;

use super::*;

#[test]
fn test_priority() {
  let l = Latch::new();
  let v = SegQueue::new();
  let c = AtomicUsize::new(0);

  let ic = 5;
  let lc = 5;
  scope(|s| {
    let _g = l.lock_immediately();

    let mut th = vec![];

    for _ in 0..lc {
      let t = s.spawn(|| {
        c.fetch_add(1, Ordering::Release);
        let _g = l.lock_lazily();
        v.push(2);
      });
      th.push(t)
    }

    for _ in 0..ic {
      let t = s.spawn(|| {
        c.fetch_add(1, Ordering::Release);
        let _g = l.lock_immediately();
        v.push(1);
      });

      th.push(t)
    }

    while c.load(Ordering::Acquire) < ic + lc {
      yield_now();
    }

    drop(_g);

    th.into_iter().for_each(|t| t.join().unwrap());
  });

  let result = vec![1, 1, 1, 1, 1, 2, 2, 2, 2, 2];
  assert_eq!(v.into_iter().collect::<Vec<_>>(), result)
}
