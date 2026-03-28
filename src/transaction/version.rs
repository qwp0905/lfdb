use std::{
  panic::RefUnwindSafe,
  sync::atomic::{AtomicU8, AtomicUsize, Ordering},
};

use crossbeam_skiplist::{map::Entry, SkipMap, SkipSet};

use crate::utils::OffsetBitmap;

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state during commit attempt — prevents timeout thread from aborting while WAL write is in progress
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

/**
 * Snapshot of the version visibility active set to achieve snapshot isolation.
 */
pub struct TxSnapshot<'a> {
  active: OffsetBitmap,
  aborted: &'a SkipSet<usize>,
}
impl<'a> TxSnapshot<'a> {
  fn new(active: &SkipMap<usize, AtomicU8>, aborted: &'a SkipSet<usize>) -> Self {
    let (front, max) = match (active.front(), active.back()) {
      (Some(front), Some(back)) => (front, *back.key()),
      _ => {
        return TxSnapshot {
          active: OffsetBitmap::new(0, 0),
          aborted,
        }
      }
    };

    let offset = *front.key();
    let mut snapshot = OffsetBitmap::new(offset, max - offset + 1);

    let mut entry = Some(front);
    while let Some(e) = entry.take_if(|e| *e.key() <= max) {
      snapshot.insert(*e.key());
      entry = e.next();
    }

    TxSnapshot {
      active: snapshot,
      aborted,
    }
  }

  #[inline]
  pub fn is_visible(&self, tx_id: &usize) -> bool {
    !self.is_active(tx_id) && !self.aborted.contains(tx_id)
  }
  #[inline]
  pub fn is_active(&self, &tx_id: &usize) -> bool {
    self.active.contains(tx_id)
  }
}

/**
 * Tracks MVCC visibility for transactions.
 *
 * Visibility is determined by exclusion: a transaction's writes are visible
 * if it is neither aborted nor still active. Committed transactions are not
 * tracked explicitly — committing simply removes the tx from active.
 */
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

  /**
   * Trims aborted tx_ids that are older than version. Called after GC completes —
   * version is the oldest tx_id that GC has fully cleaned up, so no active reader
   * can reference those versions anymore and their abort status no longer needs tracking.
   */
  pub fn remove_aborted(&self, version: &usize) {
    while let Some(v) = self.aborted.front() {
      if v.value() >= version {
        return;
      }
      v.remove();
    }
  }

  #[inline]
  pub fn is_aborted(&self, tx_id: &usize) -> bool {
    self.aborted.contains(tx_id)
  }
  /**
   * Returns the oldest active tx_id, or the current version if no transaction is active.
   * Called before GC to determine the safe cleanup boundary — versions older than this
   * are not visible to any active reader and can be collected.
   */
  pub fn min_version(&self) -> usize {
    self
      .active
      .front()
      .map(|v| *v.key())
      .unwrap_or_else(|| self.current_version())
  }
  #[inline]
  pub fn set_abort(&self, tx_id: usize) {
    self.aborted.insert(tx_id);
  }
  pub fn new_transaction(&self) -> (TxState<'_>, TxSnapshot<'_>) {
    let snapshot = TxSnapshot::new(&self.active, &self.aborted);
    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    (
      TxState(self.active.insert(tx_id, AtomicU8::new(STATUS_AVAILABLE))),
      snapshot,
    )
  }
  #[inline]
  pub fn current_version(&self) -> usize {
    self.last_tx_id.load(Ordering::Acquire)
  }
  #[inline]
  pub fn get_active_state(&self, tx_id: usize) -> Option<TxState<'_>> {
    self.active.get(&tx_id).map(TxState)
  }
}
impl RefUnwindSafe for VersionVisibility {}
