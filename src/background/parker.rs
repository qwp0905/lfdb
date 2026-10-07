use std::{
  cell::OnceCell,
  sync::{
    atomic::{AtomicU64, Ordering},
    Once,
  },
  thread::{current, park, Thread},
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

const STATE_UNQUEUED: u64 = 0;
const STATE_QUEUED: u64 = 1;
const STATE_PARKED: u64 = 2;
const STATE_CANCELED: u64 = 3;

const STATE_BITS: u32 = 2;
const STATE_MASK: u64 = (1 << STATE_BITS) - 1;

static PARKER_ID: AtomicU64 = AtomicU64::new(1);
type ParkerId = u64;

const MAX_PARKER_ID: ParkerId = ParkerId::MAX >> STATE_BITS;

const fn cast_to(v: u64) -> (ParkerId, u64) {
  (v >> STATE_BITS, v & STATE_MASK)
}
const fn cast_from(id: ParkerId, state: u64) -> u64 {
  (id << STATE_BITS) | state
}

struct Waker {
  /**
   * 62 bits parker id + 2bit state.
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

  fn try_park(&self, id: ParkerId) {
    let queued = cast_from(id, STATE_QUEUED);
    let parked = cast_from(id, STATE_PARKED);
    if self
      .state
      .compare_exchange(queued, parked, Ordering::AcqRel, Ordering::Acquire)
      .is_err()
    {
      return;
    }

    debug_assert_eq!(current().id(), self.thread.id());
    park();

    let _ = self.state.compare_exchange(
      parked,
      cast_from(id, STATE_CANCELED),
      Ordering::AcqRel,
      Ordering::Acquire,
    );
  }

  fn try_cancel(&self, id: ParkerId) -> bool {
    let queued = cast_from(id, STATE_QUEUED);
    let canceled = cast_from(id, STATE_CANCELED);
    self
      .state
      .compare_exchange(queued, canceled, Ordering::AcqRel, Ordering::Acquire)
      .is_ok()
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

thread_local! {
  static LOCAL_STATE: OnceCell<LocalState> = const { OnceCell::new() };
}

pub struct ThreadParker {
  queue: SegQueue<SBox<Waker>>,
  id: ParkerId,
}
impl ThreadParker {
  pub fn new() -> Self {
    let id = PARKER_ID.fetch_add(1, Ordering::Relaxed);
    if id > MAX_PARKER_ID {
      panic!("parker id overflowed.");
    }
    Self {
      queue: SegQueue::new(),
      id,
    }
  }
  fn try_enqueue(&self, waker: &SBox<Waker>) {
    let prev = waker
      .state
      .swap(cast_from(self.id, STATE_QUEUED), Ordering::AcqRel);
    let (id, state) = cast_to(prev);
    if id != self.id || state == STATE_UNQUEUED {
      self.queue.push(waker.clone());
    }
  }

  pub fn wake_all(&self) {
    while let Some(waker) = self.queue.pop() {
      self.wake_one(&waker);
    }
  }

  fn wake_one(&self, waker: &Waker) -> bool {
    let mut current = waker.state.load(Ordering::Acquire);
    loop {
      let (id, state) = cast_to(current);
      if id != self.id {
        return false;
      }
      if let Err(err) = waker.state.compare_exchange_weak(
        current,
        cast_from(id, STATE_UNQUEUED),
        Ordering::AcqRel,
        Ordering::Acquire,
      ) {
        current = err;
        continue;
      }

      match state {
        STATE_UNQUEUED | STATE_CANCELED => return false,
        STATE_QUEUED => return true,
        STATE_PARKED => {
          waker.thread.unpark();
          return true;
        }
        _ => unreachable!(),
      };
    }
  }

  pub fn wake_once(&self) {
    while let Some(waker) = self.queue.pop() {
      if self.wake_one(&waker) {
        return;
      }
    }
  }
  pub fn park_or_cancel<T>(&self, has_next: impl FnOnce() -> Option<T>) -> Option<T> {
    LOCAL_STATE.with(|v| {
      let local = v.get_or_init(Default::default);
      self.try_enqueue(&local.waker);
      let Some(next) = has_next() else {
        local.waker.try_park(self.id);
        return None;
      };
      if !local.waker.try_cancel(self.id) {
        self.wake_once();
      }
      Some(next)
    })
  }
}
impl Drop for ThreadParker {
  fn drop(&mut self) {
    self.wake_all();
  }
}
