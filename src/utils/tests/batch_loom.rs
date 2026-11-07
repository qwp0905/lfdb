use loom::{cell::UnsafeCell, sync::Arc, thread};

use super::*;

fn model(f: impl Fn() + Sync + Send + 'static) {
  model_with(2, f);
}

fn model_with(preemption_bound: usize, f: impl Fn() + Sync + Send + 'static) {
  let mut builder = loom::model::Builder::new();
  builder.preemption_bound = Some(preemption_bound);
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

  fn compete(&self, value: usize) {
    let Some(mut occupied) = self.queue.compete(value) else {
      return;
    };
    loop {
      let values: Vec<_> = occupied.drain_all().collect();
      self.drained.with_mut(|d| unsafe { (*d).extend(values) });
      if occupied.try_release() {
        return;
      }
    }
  }

  fn compete_one_by_one(&self, value: usize) {
    let Some(mut occupied) = self.queue.compete(value) else {
      return;
    };
    loop {
      let values: Vec<_> = occupied.drain_n(1).collect();
      self.drained.with_mut(|d| unsafe { (*d).extend(values) });
      if occupied.try_release() {
        return;
      }
    }
  }

  fn drained(&self) -> Vec<usize> {
    let mut drained = self.drained_in_order();
    drained.sort();
    drained
  }

  fn drained_in_order(&self) -> Vec<usize> {
    self.drained.with(|d| unsafe { (*d).clone() })
  }
}
unsafe impl Sync for Batch {}

fn spawn_competitor(batch: &Arc<Batch>, value: usize) -> thread::JoinHandle<()> {
  let batch = batch.clone();
  thread::spawn(move || batch.compete(value))
}

#[test]
fn every_item_is_drained_once() {
  model(|| {
    let batch = Batch::new();
    let competitor = spawn_competitor(&batch, 1);

    batch.compete(0);
    competitor.join().unwrap();

    assert_eq!(batch.drained(), vec![0, 1]);
    assert!(batch.queue.compete(2).is_some());
  });
}

#[test]
fn every_item_is_drained_once_with_three_competitors() {
  model(|| {
    let batch = Batch::new();
    let competitors = [spawn_competitor(&batch, 1), spawn_competitor(&batch, 2)];

    batch.compete(0);
    for competitor in competitors {
      competitor.join().unwrap();
    }

    assert_eq!(batch.drained(), vec![0, 1, 2]);
    assert!(batch.queue.compete(3).is_some());
  });
}

fn assert_drained_in_order(compete: fn(&Batch, usize)) {
  // Overtaking needs a push, the owner's release and the next compete to interleave.
  model_with(3, move || {
    let batch = Batch::new();
    let sequential = {
      let batch = batch.clone();
      thread::spawn(move || {
        compete(&batch, 1);
        compete(&batch, 2);
      })
    };

    compete(&batch, 0);
    sequential.join().unwrap();

    assert_eq!(batch.drained(), vec![0, 1, 2]);
    let drained = batch.drained_in_order();
    let first = drained.iter().position(|&v| v == 1);
    let second = drained.iter().position(|&v| v == 2);
    assert!(first < second, "drained out of order: {drained:?}");
  });
}

#[test]
fn items_from_one_thread_are_drained_in_order() {
  assert_drained_in_order(Batch::compete);
}

#[test]
fn items_from_one_thread_are_drained_in_order_one_by_one() {
  assert_drained_in_order(Batch::compete_one_by_one);
}
