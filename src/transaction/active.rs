use std::{
  collections::BTreeMap,
  sync::atomic::{AtomicU8, Ordering},
};

use parking_lot::RwLock;

use crate::{
  background::OnceParker,
  utils::{OffsetBitmap, SBox},
  wal::{AtomicTxId, TxId},
};

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state during commit attempt — prevents timeout thread from aborting while WAL write is in progress
const STATUS_ABORTED: u8 = 2;
const STATUS_TIMEOUT: u8 = 3;

/**
 * Active transaction status transitions.
 *
 * Every transaction starts as `AVAILABLE`.
 * - commit path:  `AVAILABLE -> ON_COMMIT`
 * - timeout path: `AVAILABLE -> TIMEOUT`
 * - abort path:   `AVAILABLE | TIMEOUT -> ABORTED`
 *
 * `ON_COMMIT` is terminal for timeout/abort ownership: once commit owns the
 * transaction, timeout code cannot abort it.
 */
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

/**
 * Active transaction registry plus close-notification primitive.
 *
 * A transaction is considered closed when it is removed from this map, regardless
 * of how it finished. Waiters parked on that transaction id are woken when the
 * state is removed.
 */
pub struct ActiveSet {
  inner: RwLock<BTreeMap<TxId, SBox<ActiveState>>>,
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

  /**
   * Allocate and publish a new active transaction under one write lock.
   *
   * A transaction id becomes externally observable only when its state is inserted
   * into the active map. Keeping id allocation and insertion in the same critical
   * section prevents a newly issued transaction from being missed by snapshots.
   */
  pub fn new_state(&self) -> SBox<ActiveState> {
    // heap allocation first without mutex
    let mut uninit = SBox::new_uninit();
    let mut inner = self.inner.write();

    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    SBox::get_mut(&mut uninit)
      .unwrap()
      .write(ActiveState::new(tx_id));

    inner
      .entry(tx_id)
      .and_modify(|_| unreachable!())
      .or_insert(unsafe { uninit.assume_init() })
      .clone()
  }
  /**
   * Build a bitmap snapshot of active transaction ids below `max`.
   *
   * The snapshot is offset-based so visibility checks can test active ids without
   * storing every possible transaction id up to `max`.
   */
  pub fn snapshot_until(&self, max: TxId) -> OffsetBitmap {
    let inner = self.inner.read();
    let Some((&offset, _)) = inner.first_key_value() else {
      return OffsetBitmap::new(0, 0);
    };
    let mut snapshot = OffsetBitmap::new(offset, max - offset + 1);
    for (id, _) in inner.range(..max) {
      snapshot.insert(*id);
    }
    snapshot
  }
  pub fn remove(&self, tx_id: &TxId) {
    let Some(state) = self.inner.write().remove(tx_id) else {
      return;
    };
    state.parker.wake_all();
  }
  pub fn min_version(&self) -> Option<TxId> {
    self.inner.read().first_key_value().map(|(k, _)| *k)
  }
  pub fn get(&self, tx_id: &TxId) -> Option<SBox<ActiveState>> {
    self.inner.read().get(tx_id).cloned()
  }
  pub fn until(&self, max: TxId) -> Vec<TxId> {
    self.inner.read().range(..max).map(|(k, _)| *k).collect()
  }
  pub fn wait(&self, tx_id: &TxId) {
    if let Some(state) = self.get(tx_id) {
      state.parker.park();
    }
  }
  pub fn get_all(&self) -> Vec<SBox<ActiveState>> {
    self.inner.read().values().cloned().collect()
  }
}
