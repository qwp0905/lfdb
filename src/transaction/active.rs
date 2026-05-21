use std::{
  collections::BTreeMap,
  sync::{
    atomic::{AtomicU8, Ordering},
    Arc, RwLock,
  },
  thread::{current, park, yield_now, Thread},
};

use crossbeam::queue::SegQueue;

use crate::{
  utils::{OffsetBitmap, ShortenedRwLock},
  wal::TxId,
};

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state during commit attempt — prevents timeout thread from aborting while WAL write is in progress
const STATUS_ABORTED: u8 = 2;
const STATUS_TIMEOUT: u8 = 3;
const STATUS_CLOSED: u8 = 4;

pub struct ActiveState {
  tx_id: TxId,
  status: AtomicU8,
  waiting: SegQueue<Thread>,
}
impl ActiveState {
  pub fn new(tx_id: TxId) -> Self {
    Self {
      tx_id,
      status: AtomicU8::new(STATUS_AVAILABLE),
      waiting: SegQueue::new(),
    }
  }
  pub fn is_available(&self) -> bool {
    self.status.load(Ordering::Acquire) == STATUS_AVAILABLE
  }
  pub fn get_id(&self) -> TxId {
    self.tx_id
  }
  pub fn close(&self) {
    self.status.store(STATUS_CLOSED, Ordering::Release);
  }

  pub fn try_abort(&self) -> bool {
    let current = self.status.load(Ordering::Acquire);
    if !matches!(current, STATUS_AVAILABLE | STATUS_TIMEOUT) {
      return false;
    }

    self
      .status
      .compare_exchange(
        current,
        STATUS_ABORTED,
        Ordering::Release,
        Ordering::Acquire,
      )
      .is_ok()
  }

  #[inline]
  pub fn try_timeout(&self) -> bool {
    self
      .status
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_TIMEOUT,
        Ordering::Release,
        Ordering::Acquire,
      )
      .is_ok()
  }

  #[inline]
  pub fn try_commit(&self) -> bool {
    self
      .status
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_ON_COMMIT,
        Ordering::Release,
        Ordering::Acquire,
      )
      .is_ok()
  }

  #[inline]
  pub fn make_available(&self) {
    self.status.store(STATUS_AVAILABLE, Ordering::Release)
  }
}

pub struct ActiveSet {
  inner: RwLock<BTreeMap<TxId, Arc<ActiveState>>>,
}
impl ActiveSet {
  pub const fn new() -> Self {
    Self {
      inner: RwLock::new(BTreeMap::new()),
    }
  }
  pub fn insert(&self, state: Arc<ActiveState>) {
    self.inner.wl().insert(state.tx_id, state);
  }
  pub fn snapshot_until(&self, max: TxId) -> OffsetBitmap {
    let inner = self.inner.rl();
    let offset = match inner.first_key_value() {
      Some((k, _)) => *k,
      None => return OffsetBitmap::new(0, 0),
    };
    let mut snapshot = OffsetBitmap::new(offset, max - offset + 1);
    for (id, _) in inner.range(..max) {
      snapshot.insert(*id);
    }
    snapshot
  }
  pub fn remove(&self, tx_id: &TxId) {
    if let Some(state) = self.inner.wl().remove(tx_id) {
      while let Some(thread) = state.waiting.pop() {
        thread.unpark();
      }
    };
  }
  pub fn min_version(&self) -> Option<TxId> {
    self.inner.rl().first_key_value().map(|(k, _)| *k)
  }
  pub fn get(&self, tx_id: &TxId) -> Option<Arc<ActiveState>> {
    self.inner.rl().get(tx_id).map(Arc::clone)
  }
  pub fn until(&self, max: TxId) -> Vec<TxId> {
    self.inner.rl().range(..max).map(|(k, _)| *k).collect()
  }
  pub fn wait(&self, tx_id: &TxId) {
    let state = match self.get(tx_id) {
      Some(state) => state,
      None => return,
    };

    let mut backoff = 0;
    state.waiting.push(current());
    loop {
      if state.status.load(Ordering::Acquire) == STATUS_CLOSED {
        return;
      }
      if backoff < MAX_BACKOFF {
        yield_now();
        backoff += 1;
        continue;
      }
      break park();
    }
  }
}

const MAX_BACKOFF: usize = 10;
