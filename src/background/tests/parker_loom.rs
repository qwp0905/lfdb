use std::collections::VecDeque;

use loom::{
  sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc, Mutex,
  },
  thread,
};

use super::*;

/**
 * Weakest queue the parker may rely on: a pop that returns a value synchronizes
 * with its push, while a pop that finds nothing synchronizes with nobody.
 */
struct WeakQueue<T> {
  items: Mutex<VecDeque<T>>,
  len: AtomicUsize,
}
impl<T> Default for WeakQueue<T> {
  fn default() -> Self {
    Self {
      items: Mutex::new(VecDeque::new()),
      len: AtomicUsize::new(0),
    }
  }
}
impl<T> WakerQueue<T> for WeakQueue<T> {
  fn push(&self, value: T) {
    let mut items = self.items.lock().unwrap();
    items.push_back(value);
    self.len.fetch_add(1, Ordering::Relaxed);
  }

  fn pop(&self) -> Option<T> {
    if self.len.load(Ordering::Relaxed) == 0 {
      return None;
    }
    let mut items = self.items.lock().unwrap();
    let value = items.pop_front()?;
    self.len.fetch_sub(1, Ordering::Relaxed);
    Some(value)
  }
}

type Parker = ThreadParker<WeakQueue<SBox<Waker>>>;

fn model(f: impl Fn() + Sync + Send + 'static) {
  let mut builder = loom::model::Builder::new();
  builder.preemption_bound = Some(2);
  builder.check(f);
}

struct Work {
  parker: Parker,
  count: AtomicUsize,
}
impl Work {
  fn new() -> Arc<Self> {
    Arc::new(Self {
      parker: Parker::new(),
      count: AtomicUsize::new(0),
    })
  }

  fn take(&self) -> Option<()> {
    let mut current = self.count.load(Ordering::Acquire);
    loop {
      if current == 0 {
        return None;
      }
      let Err(err) = self.count.compare_exchange(
        current,
        current - 1,
        Ordering::AcqRel,
        Ordering::Acquire,
      ) else {
        return Some(());
      };
      current = err;
    }
  }

  fn wait(&self) {
    while self.parker.park_or_cancel(|| self.take()).is_none() {}
  }

  fn produce(&self) {
    self.count.fetch_add(1, Ordering::Release);
    self.parker.wake_once();
  }
}

fn spawn_waiter(work: &Arc<Work>) -> thread::JoinHandle<()> {
  let work = work.clone();
  thread::spawn(move || work.wait())
}

#[test]
fn waiter_receives_produced_work() {
  model(|| {
    let work = Work::new();
    let waiter = spawn_waiter(&work);

    work.produce();
    waiter.join().unwrap();
  });
}

#[test]
fn every_waiter_receives_work() {
  model(|| {
    let work = Work::new();
    let waiters = [spawn_waiter(&work), spawn_waiter(&work)];

    work.produce();
    work.produce();
    for waiter in waiters {
      waiter.join().unwrap();
    }
  });
}

#[test]
fn wake_all_releases_every_waiter() {
  model(|| {
    let parker = Arc::new(Parker::new());
    let closed = Arc::new(AtomicBool::new(false));
    let waiters = [0, 1].map(|_| {
      let parker = parker.clone();
      let closed = closed.clone();
      thread::spawn(move || {
        while parker
          .park_or_cancel(|| closed.load(Ordering::Acquire).then_some(()))
          .is_none()
        {}
      })
    });

    closed.store(true, Ordering::Release);
    parker.wake_all();
    for waiter in waiters {
      waiter.join().unwrap();
    }
  });
}

#[test]
fn stale_entry_does_not_steal_wake_from_other_waiter() {
  model(|| {
    let first = Work::new();
    let second = Work::new();
    let moving = {
      let first = first.clone();
      let second = second.clone();
      thread::spawn(move || {
        first.wait();
        second.wait();
      })
    };
    let staying = spawn_waiter(&first);

    first.produce();
    first.produce();
    second.produce();
    moving.join().unwrap();
    staying.join().unwrap();
  });
}

#[test]
fn spurious_unpark_does_not_lose_wake() {
  model(|| {
    let work = Work::new();
    let waiter = spawn_waiter(&work);
    let unparker = {
      let thread = waiter.thread().clone();
      thread::spawn(move || thread.unpark())
    };

    work.produce();
    unparker.join().unwrap();
    waiter.join().unwrap();
  });
}
