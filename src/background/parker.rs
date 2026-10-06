use std::{
  cell::{OnceCell, RefCell},
  sync::{
    atomic::{AtomicU32, AtomicU8, Ordering},
    Once,
  },
  thread::{current, park, LocalKey, Thread},
};

use crossbeam::queue::SegQueue;

use crate::utils::SBox;

/**
 * A tiny one-shot wake-all primitive.
 *
 * `park` waits until the parker is opened, and `wake_all` opens it
 * permanently. Once opened, all current waiters are released and future calls
 * to `park` return immediately.
 *
 * This is a small utility around `Once` used as a wake-all latch. It is not
 * tied to a specific background runtime and could live in the general utility
 * module. Poisoning is intentionally ignored: a panic while opening the latch is
 * treated as a severe programming error, not as a recoverable state.
 */
pub struct OnceParker(Once);

impl OnceParker {
  pub const fn new() -> Self {
    Self(Once::new())
  }

  pub fn park(&self) {
    self.0.wait_force();
  }

  pub fn wake_all(&self) {
    self.0.call_once_force(|_| ());
  }
}
impl Default for OnceParker {
  fn default() -> Self {
    Self::new()
  }
}

const STATE_UNQUEUED: u8 = 0;
const STATE_QUEUED: u8 = 1;
const STATE_PARKED: u8 = 2;
const STATE_CANCELED: u8 = 3;

static PARKER_ID: AtomicU32 = AtomicU32::new(0);
type ParkerId = u32;

struct Waker {
  state: AtomicU8,
  thread: Thread,
}
impl Waker {
  fn new() -> Self {
    Self {
      state: AtomicU8::new(STATE_UNQUEUED),
      thread: current(),
    }
  }

  fn try_park(&self) {
    if self
      .state
      .compare_exchange(
        STATE_QUEUED,
        STATE_PARKED,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_err()
    {
      return;
    }

    debug_assert_eq!(current().id(), self.thread.id());
    park();
    let _ = self.state.compare_exchange(
      STATE_PARKED,
      STATE_QUEUED,
      Ordering::Release,
      Ordering::Acquire,
    );
  }

  fn try_cancel(&self) -> bool {
    self
      .state
      .compare_exchange(
        STATE_QUEUED,
        STATE_CANCELED,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
  }
}

pub struct LocalState {
  waker: SBox<Waker>,
  parker_id: ParkerId,
}
impl LocalState {
  fn new(parker_id: ParkerId) -> Self {
    Self {
      waker: SBox::new(Waker::new()),
      parker_id,
    }
  }
}

macro_rules! create_parker {
  () => {{
    use crate::background::LocalState;
    use std::cell::{OnceCell, RefCell};
    thread_local! {
      static LOCAL: OnceCell<RefCell<LocalState>> = const { OnceCell::new() };
    }
    ThreadParker::new(&LOCAL)
  }};
}
pub(crate) use create_parker;

pub struct ThreadParker {
  queue: SegQueue<SBox<Waker>>,
  local: &'static LocalKey<OnceCell<RefCell<LocalState>>>,
  id: ParkerId,
}
impl ThreadParker {
  pub fn new(local: &'static LocalKey<OnceCell<RefCell<LocalState>>>) -> Self {
    Self {
      queue: SegQueue::new(),
      local,
      id: PARKER_ID.fetch_add(1, Ordering::Relaxed),
    }
  }
  fn create_local(&self) -> LocalState {
    LocalState::new(self.id)
  }
  fn try_enqueue(&self, waker: &SBox<Waker>) {
    match waker.state.swap(STATE_QUEUED, Ordering::AcqRel) {
      STATE_UNQUEUED => self.queue.push(waker.clone()),
      STATE_QUEUED | STATE_CANCELED => {}
      _ => unreachable!(),
    }
  }

  pub fn wake_all(&self) {
    while let Some(waker) = self.queue.pop() {
      let state = waker.state.swap(STATE_UNQUEUED, Ordering::AcqRel);
      match state {
        STATE_QUEUED => {}
        STATE_PARKED => waker.thread.unpark(),
        STATE_CANCELED => {}
        _ => unreachable!(),
      }
    }
  }

  pub fn wake_once(&self) {
    while let Some(waker) = self.queue.pop() {
      let state = waker.state.swap(STATE_UNQUEUED, Ordering::AcqRel);
      match state {
        STATE_QUEUED => return,
        STATE_PARKED => return waker.thread.unpark(),
        STATE_CANCELED => continue,
        _ => unreachable!(),
      }
    }
  }
  pub fn park_or_cancel<T>(&self, has_next: impl FnOnce() -> Option<T>) -> Option<T> {
    self.local.with(|v| {
      let mut local = v
        .get_or_init(|| RefCell::new(self.create_local()))
        .borrow_mut();
      if local.parker_id != self.id {
        *local = self.create_local();
      };
      self.try_enqueue(&local.waker);
      let Some(next) = has_next() else {
        local.waker.try_park();
        return None;
      };
      if !local.waker.try_cancel() {
        self.wake_once();
      }
      Some(next)
    })
  }
}
impl Drop for ThreadParker {
  fn drop(&mut self) {
    while !self.queue.is_empty() {
      self.wake_once();
    }
  }
}
