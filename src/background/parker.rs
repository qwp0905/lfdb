use std::{
  cell::OnceCell,
  sync::{atomic::Ordering, Once},
};

use crossbeam::queue::SegQueue;

use crate::utils::{
  loom::{
    fence,
    thread::{current, park, Thread},
    thread_local, AtomicU64,
  },
  SBox,
};

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

const STATE_UNQUEUED: u64 = 0;
const STATE_QUEUED: u64 = 1;
const STATE_PARKED: u64 = 2;
const STATE_CANCELED: u64 = 3;

const STATE_BITS: u32 = 2;
const STATE_MASK: u64 = (1 << STATE_BITS) - 1;

// Ids only have to be unique, so the counter stays outside the loom model.
static PARKER_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
type ParkerId = u64;

const MAX_PARKER_ID: ParkerId = ParkerId::MAX >> STATE_BITS;

const fn cast_to(v: u64) -> (ParkerId, u64) {
  (v >> STATE_BITS, v & STATE_MASK)
}
const fn cast_from(id: ParkerId, state: u64) -> u64 {
  (id << STATE_BITS) | state
}

pub(crate) struct Waker {
  /**
   * 62 bits parker id + 2bit state.
   *
   * Transitions only decide who owns a wake; visibility of the work is carried
   * by the fences in `ThreadParker`.
   */
  state: AtomicU64,
  thread: Thread,
}
impl Waker {
  fn new() -> Self {
    Self {
      state: AtomicU64::new(0),
      thread: current(),
    }
  }

  fn park_by(&self, id: ParkerId) {
    let queued = cast_from(id, STATE_QUEUED);
    let parked = cast_from(id, STATE_PARKED);
    if self
      .state
      .compare_exchange(queued, parked, Ordering::Relaxed, Ordering::Relaxed)
      .is_err()
    {
      return;
    }

    debug_assert_eq!(current().id(), self.thread.id());
    park();

    let _ = self.state.compare_exchange(
      parked,
      cast_from(id, STATE_CANCELED),
      Ordering::Relaxed,
      Ordering::Relaxed,
    );
  }

  fn cancel_by(&self, id: ParkerId) -> bool {
    let queued = cast_from(id, STATE_QUEUED);
    let canceled = cast_from(id, STATE_CANCELED);
    self
      .state
      .compare_exchange(queued, canceled, Ordering::Relaxed, Ordering::Relaxed)
      .is_ok()
  }

  fn wake_by(&self, parker_id: ParkerId) -> bool {
    let mut current = self.state.load(Ordering::Relaxed);
    loop {
      let (id, state) = cast_to(current);
      if id != parker_id {
        return false;
      }
      if let Err(err) = self.state.compare_exchange_weak(
        current,
        cast_from(id, STATE_UNQUEUED),
        Ordering::Relaxed,
        Ordering::Relaxed,
      ) {
        current = err;
        continue;
      }

      match state {
        STATE_UNQUEUED | STATE_CANCELED => return false,
        STATE_QUEUED => return true,
        STATE_PARKED => {
          self.thread.unpark();
          return true;
        }
        _ => unreachable!(),
      };
    }
  }
}

struct LocalState {
  waker: SBox<Waker>,
}
impl LocalState {
  fn new() -> Self {
    Self {
      waker: SBox::new(Waker::new()),
    }
  }
}
impl Default for LocalState {
  fn default() -> Self {
    Self::new()
  }
}

#[cfg(not(loom))]
thread_local! {
  static LOCAL_STATE: OnceCell<LocalState> = const { OnceCell::new() };
}
// Loom's thread_local does not accept a const initializer.
#[cfg(loom)]
thread_local! {
  static LOCAL_STATE: OnceCell<LocalState> = OnceCell::new();
}

/**
 * The parker relies on the queue only for happens-before from push to the pop
 * that returns the value, so it stays correct regardless of the implementation.
 */
pub(crate) trait WakerQueue<T>: Default {
  fn push(&self, value: T);
  fn pop(&self) -> Option<T>;
}
impl<T> WakerQueue<T> for SegQueue<T> {
  fn push(&self, value: T) {
    SegQueue::push(self, value);
  }
  fn pop(&self) -> Option<T> {
    SegQueue::pop(self)
  }
}

pub struct ThreadParker<Q: WakerQueue<SBox<Waker>> = SegQueue<SBox<Waker>>> {
  queue: Q,
  id: ParkerId,
}
impl<Q: WakerQueue<SBox<Waker>>> ThreadParker<Q> {
  pub fn new() -> Self {
    let id = PARKER_ID.fetch_add(1, Ordering::Relaxed);
    assert!(id <= MAX_PARKER_ID, "parker id overflowed.");
    Self {
      queue: Q::default(),
      id,
    }
  }
  fn try_enqueue(&self, waker: &SBox<Waker>) {
    let prev = waker
      .state
      .swap(cast_from(self.id, STATE_QUEUED), Ordering::Relaxed);
    let (id, state) = cast_to(prev);
    if id != self.id || state == STATE_UNQUEUED {
      self.queue.push(waker.clone());
    }
  }

  pub fn wake_all(&self) {
    fence(Ordering::SeqCst);
    while let Some(waker) = self.queue.pop() {
      waker.wake_by(self.id);
    }
  }

  pub fn wake_once(&self) {
    fence(Ordering::SeqCst);
    while let Some(waker) = self.queue.pop() {
      if waker.wake_by(self.id) {
        return;
      }
    }
  }
  pub fn park_or_cancel<T>(&self, has_next: impl FnOnce() -> Option<T>) -> Option<T> {
    LOCAL_STATE.with(|v| {
      let local = v.get_or_init(Default::default);
      self.try_enqueue(&local.waker);
      // Pairs with the fence in wake, so either the waker finds this entry or
      // has_next sees the work published before the wake.
      fence(Ordering::SeqCst);
      let Some(next) = has_next() else {
        local.waker.park_by(self.id);
        return None;
      };
      if !local.waker.cancel_by(self.id) {
        self.wake_once();
      }
      Some(next)
    })
  }
}
impl<Q: WakerQueue<SBox<Waker>>> Drop for ThreadParker<Q> {
  fn drop(&mut self) {
    self.wake_all();
  }
}

#[cfg(all(test, loom))]
#[path = "tests/parker_loom.rs"]
mod tests;
