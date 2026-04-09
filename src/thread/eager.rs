use std::{
  cell::UnsafeCell,
  mem::ManuallyDrop,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
};

use crate::{
  thread::{SharedFn, TaskHandle},
  utils::{ToArc, UnsafeBorrow, UnsafeBorrowMut},
  Result,
};

use super::{BackgroundThread, Context, OneshotFulfill, Spawner};
use crossbeam::{queue::SegQueue, utils::Backoff};

/**
 * The callback is called once per batch and the result is cloned to each
 * waiter — hence R: Clone.
 */
fn make_flush<'a, T, R>(
  when_buffered: SharedFn<'a, Vec<T>, R>,
) -> impl Fn(&mut Vec<(T, OneshotFulfill<Result<R>>)>) + 'a
where
  T: Send + UnwindSafe + 'static,
  R: Send + Clone + 'static,
{
  move |buffered| {
    if buffered.is_empty() {
      return;
    }

    let (values, waiting): (Vec<_>, Vec<_>) = buffered.drain(..).unzip();
    let result = when_buffered.call(values).map(Ok).unwrap_or_else(Err);
    waiting
      .into_iter()
      .for_each(|done| done.fulfill(result.clone()));
  }
}

/**
 * A background thread that batches incoming work items and flushes them
 * together via a single callback call.
 *
 * While a flush is in progress, new items accumulate in the queue.
 * After each flush, the thread immediately drains the queue for the next
 * batch — achieving continuous pipelining without idle gaps.
 *
 * The burst loop uses backoff to spin briefly before parking, allowing
 * items that arrive just after a flush to be included in the next batch
 * rather than triggering a separate wakeup.
 */
pub struct EagerBufferingThread<T, R> {
  queue: Arc<SegQueue<Context<T, R>>>,
  count: usize,
  work: UnsafeCell<ManuallyDrop<SharedFn<'static, Vec<T>, R>>>,
  spawner: Arc<Spawner>,
  alive: Arc<AtomicUsize>,
  current: UnsafeCell<Option<TaskHandle<()>>>,
  closed: AtomicBool,
}
impl<T, R> EagerBufferingThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + Clone + 'static,
{
  pub fn new(
    spawner: Arc<Spawner>,
    count: usize,
    when_buffered: SharedFn<'static, Vec<T>, R>,
  ) -> Self {
    Self {
      queue: SegQueue::new().to_arc(),
      work: UnsafeCell::new(ManuallyDrop::new(when_buffered)),
      count,
      alive: AtomicUsize::new(0).to_arc(),
      spawner,
      current: UnsafeCell::new(None),
      closed: AtomicBool::new(false),
    }
  }

  fn spawn(&self) {
    let alive = self.alive.clone();
    let count = self.count;
    let queue = self.queue.clone();
    let flush = make_flush(SharedFn::clone(self.work.get().borrow_unsafe()));

    let handle = self.spawner.spawn(move || {
      let backoff = Backoff::new();
      let mut buffered = Vec::with_capacity(count);

      loop {
        'inner: while buffered.len() < count {
          match queue.pop() {
            Some(Context::Work(v, done)) => {
              alive.fetch_sub(1, Ordering::Release);
              buffered.push((v, done));
            }
            Some(Context::Term) => return flush(&mut buffered),
            None => break 'inner,
          }
        }

        flush(&mut buffered);

        loop {
          let current = alive.load(Ordering::Acquire);
          if current & !ALIVE_BIT > 0 {
            break;
          }

          if alive
            .compare_exchange(current, 0, Ordering::Release, Ordering::Acquire)
            .is_ok()
          {
            return;
          }

          backoff.spin()
        }
        backoff.reset()
      }
    });

    unsafe { self.current.get().write(Some(handle)) };
  }
}
impl<T, R> UnwindSafe for EagerBufferingThread<T, R> {}
impl<T, R> RefUnwindSafe for EagerBufferingThread<T, R> {}
unsafe impl<T, R> Send for EagerBufferingThread<T, R> {}
unsafe impl<T, R> Sync for EagerBufferingThread<T, R> {}

const ALIVE_BIT: usize = 1 << (usize::BITS - 1);

impl<T, R> BackgroundThread<T, R> for EagerBufferingThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + Clone + 'static,
{
  fn register(&self, ctx: Context<T, R>) -> bool {
    if self.closed.load(Ordering::Acquire) {
      return false;
    }

    self.queue.push(ctx);
    let backoff = Backoff::new();
    loop {
      let cur = self.alive.load(Ordering::Acquire);
      let new = (cur + 1) | ALIVE_BIT;
      if let Ok(old) =
        self
          .alive
          .compare_exchange(cur, new, Ordering::Release, Ordering::Acquire)
      {
        if old & ALIVE_BIT == 0 {
          self.spawn();
        }
        return true;
      }

      backoff.spin()
    }
  }

  fn terminate(&self) {
    if self.closed.fetch_or(true, Ordering::Release) {
      return;
    };
    if let Some(handle) = self.current.get().borrow_mut_unsafe().take() {
      let _ = handle.wait();
    }
    unsafe { ManuallyDrop::drop(self.work.get().borrow_mut_unsafe()) }
  }
}

#[cfg(test)]
#[path = "tests/eager.rs"]
mod tests;
