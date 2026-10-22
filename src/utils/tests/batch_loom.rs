use loom::{cell::UnsafeCell, sync::Arc, thread};

use super::*;

fn model(f: impl Fn() + Sync + Send + 'static) {
  let mut builder = loom::model::Builder::new();
  builder.preemption_bound = Some(2);
  builder.check(f);
}

struct Batch {
  queue: BatchQueue<usize>,
  drained: UnsafeCell<Vec<usize>>,
}
impl Batch {
  fn new() -> Arc<Self> {
    Arc::new(Self {
      queue: BatchQueue::new(),
      drained: UnsafeCell::new(Vec::new()),
    })
  }

  fn push(&self, value: usize) {
    if !self.queue.push_and_compete(value) {
      return;
    }
    loop {
      let values: Vec<_> = unsafe { self.queue.drain_all() }.collect();
      self.drained.with_mut(|d| unsafe { (*d).extend(values) });
      if self.queue.try_release() {
        return;
      }
    }
  }

  fn drained(&self) -> Vec<usize> {
    let mut drained = self.drained.with(|d| unsafe { (*d).clone() });
    drained.sort();
    drained
  }
}
unsafe impl Sync for Batch {}

fn spawn_pusher(batch: &Arc<Batch>, value: usize) -> thread::JoinHandle<()> {
  let batch = batch.clone();
  thread::spawn(move || batch.push(value))
}

#[test]
fn every_pushed_item_is_drained_once() {
  model(|| {
    let batch = Batch::new();
    let pusher = spawn_pusher(&batch, 1);

    batch.push(0);
    pusher.join().unwrap();

    assert_eq!(batch.drained(), vec![0, 1]);
    assert!(batch.queue.push_and_compete(2));
  });
}

#[test]
fn every_pushed_item_is_drained_once_with_three_pushers() {
  model(|| {
    let batch = Batch::new();
    let pushers = [spawn_pusher(&batch, 1), spawn_pusher(&batch, 2)];

    batch.push(0);
    for pusher in pushers {
      pusher.join().unwrap();
    }

    assert_eq!(batch.drained(), vec![0, 1, 2]);
    assert!(batch.queue.push_and_compete(3));
  });
}
