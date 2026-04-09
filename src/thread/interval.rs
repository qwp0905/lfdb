use std::{
  cell::UnsafeCell,
  mem::ManuallyDrop,
  panic::{RefUnwindSafe, UnwindSafe},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
  },
  time::Duration,
};

use crate::{
  thread::spawner::TaskHandle,
  utils::{ShortenedMutex, ToArc, UnsafeBorrow, UnsafeBorrowMut},
};

use super::{BackgroundThread, Context, SharedFn, Spawner, TimerThread};

struct IntervalState {
  generation: usize,
  handle: Option<TaskHandle<()>>,
  running: bool,
}
impl IntervalState {
  fn uninit() -> Self {
    Self {
      generation: 0,
      handle: None,
      running: false,
    }
  }
}

/**
 * A background thread that processes work items on demand, and also calls
 * the work function periodically with None when no item arrives within
 * the timeout — useful for recurring maintenance tasks like GC or flush.
 */
pub struct IntervalWorkThread<T, R> {
  work: UnsafeCell<ManuallyDrop<SharedFn<'static, Option<T>, R>>>,
  timer: Arc<TimerThread>,
  spawner: Arc<Spawner>,
  state: Arc<Mutex<IntervalState>>,
  timeout: Duration,
  closed: AtomicBool,
}
impl<T, R> IntervalWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  pub fn new(
    timer: Arc<TimerThread>,
    spawner: Arc<Spawner>,
    timeout: Duration,
    work: SharedFn<'static, Option<T>, R>,
  ) -> Self {
    let current = 0;
    let state = Mutex::new(IntervalState::uninit()).to_arc();
    let handle = timer.clone().register(
      Self::recursive_call(
        spawner.clone(),
        timer.clone(),
        timeout,
        state.clone(),
        current,
        work.clone(),
      ),
      timeout,
    );
    state.l().handle = Some(handle);
    Self {
      work: UnsafeCell::new(ManuallyDrop::new(work)),
      timer,
      spawner,
      state,
      timeout,
      closed: AtomicBool::new(false),
    }
  }

  fn recursive_call(
    spawner: Arc<Spawner>,
    timer: Arc<TimerThread>,
    timeout: Duration,
    state: Arc<Mutex<IntervalState>>,
    current: usize,
    work: SharedFn<'static, Option<T>, R>,
  ) -> impl FnOnce() + Send + UnwindSafe {
    move || {
      {
        let mut s = state.l();
        if s.generation != current {
          return;
        }
        s.running = true;
      }

      let _ = work.call(None);

      let ss = state.clone();
      let mut s = state.l();
      if s.generation != current {
        return;
      }
      s.running = false;
      let handle = timer.clone().register(
        Self::recursive_call(spawner, timer, timeout, ss, current, work),
        timeout,
      );
      s.handle = Some(handle);
    }
  }
}
unsafe impl<T, R> Send for IntervalWorkThread<T, R> {}
unsafe impl<T, R> Sync for IntervalWorkThread<T, R> {}
impl<T, R> RefUnwindSafe for IntervalWorkThread<T, R> {}
impl<T, R> UnwindSafe for IntervalWorkThread<T, R> {}

impl<T, R> BackgroundThread<T, R> for IntervalWorkThread<T, R>
where
  T: Send + UnwindSafe + 'static,
  R: Send + 'static,
{
  fn register(&self, ctx: Context<T, R>) -> bool {
    if self.closed.load(Ordering::Acquire) {
      return false;
    }
    let mut slock = self.state.l();
    let current = slock.generation + 1;
    let prev = match slock.running {
      true => slock.handle.take(),
      false => None,
    };

    let work = SharedFn::clone(self.work.get().borrow_unsafe());
    let timer = self.timer.clone();
    let timeout = self.timeout;
    let spawner = self.spawner.clone();
    let state = self.state.clone();

    let handle = self.spawner.spawn(move || {
      if let Some(prev) = prev {
        let _ = prev.wait();
      };

      match ctx {
        Context::Work(v, fulfill) => fulfill.fulfill(work.call(Some(v))),
        Context::Term => return,
      }

      let state_c = state.clone();
      let mut s = state.l();
      if s.generation != current {
        return;
      }
      s.running = false;
      let handle = timer.clone().register(
        Self::recursive_call(spawner, timer, timeout, state_c, current, work),
        timeout,
      );
      s.handle = Some(handle);
    });

    slock.running = true;
    slock.generation = current;
    slock.handle = Some(handle);

    true
  }

  fn terminate(&self) {
    if self.closed.fetch_or(true, Ordering::Release) {
      return;
    }

    let handle = self.state.l().handle.take();
    if let Some(h) = handle {
      let _ = h.wait();
    }

    unsafe { ManuallyDrop::drop(self.work.get().borrow_mut_unsafe()) };
  }
}

#[cfg(test)]
#[path = "tests/interval.rs"]
mod tests;
