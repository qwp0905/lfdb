use std::{
  sync::atomic::{AtomicBool, AtomicU16, AtomicU8, Ordering},
  thread::yield_now,
};

const MASK: u8 = u16::BITS as u8 - 1;
const FULL: u16 = u16::MAX;

pub struct WaitHistory {
  bits: AtomicU16,
  head: AtomicU8,
  exceeded: AtomicBool,
}
impl WaitHistory {
  pub const fn new() -> Self {
    Self {
      bits: AtomicU16::new(0),
      head: AtomicU8::new(0),
      exceeded: AtomicBool::new(false),
    }
  }

  pub fn current(&self) -> Backoff<'_> {
    let bits = self.bits.load(Ordering::Relaxed);
    let i = self.head.fetch_add(1, Ordering::Relaxed) & MASK;
    if self.exceeded.load(Ordering::Relaxed) {
      return Backoff::new(self, State::Exceeded, i);
    }

    if bits == FULL {
      self.exceeded.store(true, Ordering::Release);
      return Backoff::new(self, State::Exceeded, i);
    }

    Backoff::new(self, State::Try(crossbeam::utils::Backoff::new()), i)
  }

  fn remove_bit(&self, i: u8) {
    let bit = 1 << i;
    if bit == self.bits.fetch_and(!bit, Ordering::Relaxed) {
      self.exceeded.store(false, Ordering::Relaxed);
    };
  }
  fn set_bit(&self, i: u8) {
    let bit = 1 << i;
    self.bits.fetch_or(bit, Ordering::Relaxed);
  }
}

enum State {
  Try(crossbeam::utils::Backoff),
  Exceeded,
}

pub struct Backoff<'a> {
  history: &'a WaitHistory,
  state: State,
  index: u8,
}
impl<'a> Backoff<'a> {
  const fn new(history: &'a WaitHistory, state: State, index: u8) -> Self {
    Self {
      history,
      state,
      index,
    }
  }

  pub fn is_completed(&self) -> bool {
    match &self.state {
      State::Try(backoff) => backoff.is_completed(),
      State::Exceeded => true,
    }
  }

  pub fn snooze(&self) {
    match &self.state {
      State::Try(backoff) => backoff.snooze(),
      State::Exceeded => yield_now(),
    }
  }
}
impl<'a> Drop for Backoff<'a> {
  fn drop(&mut self) {
    match &self.state {
      State::Try(backoff) if backoff.is_completed() => self.history.set_bit(self.index),
      State::Try(_) | State::Exceeded => self.history.remove_bit(self.index),
    }
  }
}
