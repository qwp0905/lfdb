use std::sync::atomic::{AtomicU8, Ordering};

use crossbeam::queue::SegQueue;

/*
 * The worker is not discoverable through the idle queue.
 *
 * Producers cannot wake this worker directly through `idle`; either it is
 * running, or it will re-register itself before sleeping.
 */
const STATE_UNQUEUED: u8 = 0;
/*
 * The worker has published itself to the idle queue and is preparing to park.
 *
 * It is still checking for work. If a producer observes this state and changes
 * it back to `Unqueued`, the worker will notice that signal and avoid parking.
 */
const STATE_QUEUED: u8 = 1;
/*
 * The worker found no work after publishing itself and has gone to sleep.
 *
 * A producer that takes this idle entry must unpark the corresponding thread.
 */
const STATE_PARKED: u8 = 2;

pub type ThreadId = usize;

pub struct IdleQueue {
  idle: SegQueue<ThreadId>,
  states: Box<[AtomicU8]>,
}
impl IdleQueue {
  pub fn new(size: usize) -> Self {
    let mut states = Vec::with_capacity(size);
    states.resize_with(size, || AtomicU8::new(STATE_UNQUEUED));
    Self {
      idle: SegQueue::new(),
      states: states.into_boxed_slice(),
    }
  }

  pub fn try_enqueue(&self, id: ThreadId) {
    if self.states[id]
      .compare_exchange(
        STATE_UNQUEUED,
        STATE_QUEUED,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
    {
      // there are no state in idle queue.
      self.idle.push(id);
    }
  }

  pub fn try_park(&self, id: ThreadId) -> bool {
    // if producer changed state, then never park.
    self.states[id]
      .compare_exchange(
        STATE_QUEUED,
        STATE_PARKED,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
  }

  pub fn wake_one(&self) -> Option<ThreadId> {
    let id = self.idle.pop()?;
    // if does not matches parked, worker thread are already working.
    if let STATE_PARKED = self.states[id].swap(STATE_UNQUEUED, Ordering::AcqRel) {
      return Some(id);
    }
    None
  }
}
