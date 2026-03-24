use std::{
  panic::RefUnwindSafe,
  sync::atomic::{AtomicU8, AtomicUsize, Ordering},
};

use crossbeam_skiplist::{map::Entry, SkipMap, SkipSet};

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state prior to commit
const STATUS_COMMITTED: u8 = 2; // The commit log has been successfully written.
const STATUS_ABORTED: u8 = 3;

pub struct TxState<'a>(Entry<'a, usize, AtomicU8>);
impl<'a> TxState<'a> {
  #[inline(always)]
  pub fn get_id(&self) -> usize {
    *self.0.key()
  }
  #[inline]
  pub fn is_available(&self) -> bool {
    self.0.value().load(Ordering::Acquire) == STATUS_AVAILABLE
  }
  #[inline]
  pub fn try_abort(&self) -> bool {
    self
      .0
      .value()
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_ABORTED,
        Ordering::Release,
        Ordering::Acquire,
      )
      .is_ok()
  }

  #[inline]
  pub fn try_commit(&self) -> bool {
    self
      .0
      .value()
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_ON_COMMIT,
        Ordering::Release,
        Ordering::Acquire,
      )
      .is_ok()
  }

  #[inline(always)]
  pub fn deactive(&self) {
    self.0.remove();
  }

  #[inline]
  pub fn complete_commit(&self) {
    self.0.value().store(STATUS_COMMITTED, Ordering::Release)
  }
  #[inline]
  pub fn make_available(&self) {
    self.0.value().store(STATUS_AVAILABLE, Ordering::Release)
  }
}

pub struct VersionVisibility {
  aborted: SkipSet<usize>,
  active: SkipMap<usize, AtomicU8>,
  last_tx_id: AtomicUsize,
}
impl VersionVisibility {
  pub fn new<T>(aborted: T, last_tx_id: usize) -> Self
  where
    T: IntoIterator<Item = usize>,
  {
    Self {
      active: Default::default(),
      aborted: SkipSet::from_iter(aborted),
      last_tx_id: AtomicUsize::new(last_tx_id),
    }
  }

  pub fn remove_aborted(&self, version: &usize) {
    while let Some(v) = self.aborted.front() {
      if v.value() >= version {
        return;
      }
      v.remove();
    }
  }

  pub fn is_aborted(&self, tx_id: &usize) -> bool {
    self.aborted.contains(tx_id)
  }
  pub fn min_version(&self) -> usize {
    self
      .active
      .front()
      .map(|v| *v.key())
      .unwrap_or_else(|| self.current_version())
  }
  pub fn is_visible(&self, tx_id: &usize) -> bool {
    !self.aborted.contains(tx_id) && !self.active.contains_key(tx_id)
  }
  pub fn is_active(&self, tx_id: &usize) -> bool {
    self.active.contains_key(tx_id)
  }
  pub fn set_abort(&self, tx_id: usize) {
    self.aborted.insert(tx_id);
  }
  pub fn new_transaction(&self) -> TxState<'_> {
    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    TxState(self.active.insert(tx_id, AtomicU8::new(STATUS_AVAILABLE)))
  }
  pub fn current_version(&self) -> usize {
    self.last_tx_id.load(Ordering::Acquire)
  }
  pub fn get_active_state(&self, tx_id: usize) -> Option<TxState<'_>> {
    self.active.get(&tx_id).map(TxState)
  }
}
impl RefUnwindSafe for VersionVisibility {}
