use loom::{
  sync::{atomic::AtomicUsize, Arc},
  thread,
};

use super::*;

// The spin loops in push and pop make unbounded preemption exploration intractable.
fn model(f: impl Fn() + Sync + Send + 'static) {
  let mut builder = loom::model::Builder::new();
  builder.preemption_bound = Some(2);
  builder.check(f);
}

fn spawn_producers(
  queue: &Arc<MpscQueue<usize>>,
  per_producer: usize,
) -> Vec<thread::JoinHandle<()>> {
  (0..2)
    .map(|producer| {
      let queue = queue.clone();
      thread::spawn(move || {
        for i in 0..per_producer {
          queue.push(producer * 10 + i);
        }
      })
    })
    .collect()
}

fn drain(queue: &MpscQueue<usize>, count: usize) -> Vec<usize> {
  let mut values = Vec::with_capacity(count);
  while values.len() < count {
    match unsafe { queue.pop() } {
      Some(value) => values.push(value),
      None => thread::yield_now(),
    }
  }
  values
}

fn assert_complete_and_ordered(values: &[usize], per_producer: usize) {
  for producer in 0..2 {
    let received: Vec<_> = values
      .iter()
      .filter(|&&v| v / 10 == producer)
      .map(|&v| v % 10)
      .collect();
    assert_eq!(received, (0..per_producer).collect::<Vec<_>>());
  }
}

#[test]
fn concurrent_pushes_are_received_once_in_order() {
  model(|| {
    let queue = Arc::new(MpscQueue::new());
    let producers = spawn_producers(&queue, 1);

    let values = drain(&queue, 2);
    for producer in producers {
      producer.join().unwrap();
    }

    assert_complete_and_ordered(&values, 1);
    assert!(unsafe { queue.pop() }.is_none());
  });
}

#[test]
fn pushes_across_block_boundary_are_received_once_in_order() {
  model(|| {
    let queue = Arc::new(MpscQueue::new());
    let producers = spawn_producers(&queue, 2);

    let values = drain(&queue, 4);
    for producer in producers {
      producer.join().unwrap();
    }

    assert_complete_and_ordered(&values, 2);
    assert!(unsafe { queue.pop() }.is_none());
  });
}

#[test]
fn pop_racing_with_push_does_not_lose_value() {
  model(|| {
    let queue = Arc::new(MpscQueue::new());
    let producer = {
      let queue = queue.clone();
      thread::spawn(move || queue.push(1))
    };

    let first = unsafe { queue.pop() };
    producer.join().unwrap();
    let second = unsafe { queue.pop() };

    assert_eq!(first.xor(second), Some(1));
    assert!(unsafe { queue.pop() }.is_none());
  });
}

struct Tracked {
  id: usize,
  drops: Arc<Vec<AtomicUsize>>,
}
impl Drop for Tracked {
  fn drop(&mut self) {
    self.drops[self.id].fetch_add(1, Ordering::Relaxed);
  }
}

#[test]
fn drop_releases_each_remaining_value_once() {
  model(|| {
    let drops = Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let queue = Arc::new(MpscQueue::new());
    let producers: Vec<_> = (0..2)
      .map(|producer| {
        let queue = queue.clone();
        let drops = drops.clone();
        thread::spawn(move || {
          for i in 0..2 {
            let id = producer * 2 + i;
            queue.push(Tracked {
              id,
              drops: drops.clone(),
            });
          }
        })
      })
      .collect();

    drop(unsafe { queue.pop() });
    for producer in producers {
      producer.join().unwrap();
    }
    drop(queue);

    for count in drops.iter() {
      assert_eq!(count.load(Ordering::Relaxed), 1);
    }
  });
}
