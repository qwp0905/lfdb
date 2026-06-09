use std::{
  collections::BTreeMap,
  sync::{
    atomic::{AtomicU8, Ordering},
    Arc, RwLock,
  },
};

use crate::{
  background::OnceParker,
  utils::{OffsetBitmap, ShortenedRwLock},
  wal::{AtomicTxId, TxId},
};

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state during commit attempt — prevents timeout thread from aborting while WAL write is in progress
const STATUS_ABORTED: u8 = 2;
const STATUS_TIMEOUT: u8 = 3;

pub struct ActiveState {
  tx_id: TxId,
  status: AtomicU8,
  parker: OnceParker,
}
impl ActiveState {
  pub const fn new(tx_id: TxId) -> Self {
    Self {
      tx_id,
      status: AtomicU8::new(STATUS_AVAILABLE),
      parker: OnceParker::new(),
    }
  }
  pub fn is_available(&self) -> bool {
    self.status.load(Ordering::Acquire) == STATUS_AVAILABLE
  }
  pub const fn get_id(&self) -> TxId {
    self.tx_id
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
  last_tx_id: AtomicTxId,
}
impl ActiveSet {
  pub const fn new(last_tx_id: TxId) -> Self {
    Self {
      inner: RwLock::new(BTreeMap::new()),
      last_tx_id: AtomicTxId::new(last_tx_id),
    }
  }
  pub fn current_version(&self) -> TxId {
    self.last_tx_id.load(Ordering::Acquire)
  }
  pub fn new_state(&self) -> Arc<ActiveState> {
    let mut uninit = Arc::<ActiveState>::new_uninit();
    let mut inner = self.inner.wl();

    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    Arc::get_mut(&mut uninit)
      .unwrap()
      .write(ActiveState::new(tx_id));

    inner
      .entry(tx_id)
      .or_insert(unsafe { uninit.assume_init() })
      .clone()
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
    let state = match self.inner.wl().remove(tx_id) {
      Some(state) => state,
      None => return,
    };
    state.parker.wake_all();
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
    if let Some(state) = self.get(tx_id) {
      state.parker.park();
    }
  }
}
