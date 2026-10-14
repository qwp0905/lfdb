use std::{
  collections::BTreeMap,
  sync::{
    atomic::{AtomicU8, Ordering},
    LazyLock, RwLock,
  },
};

use crate::{
  background::OnceParker,
  utils::{OffsetBitmap, SBox, ShortenedRwLock},
  wal::{AtomicTxId, TxId},
};

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state during commit attempt — prevents aborting while WAL write is in progress
const STATUS_ABORTED: u8 = 2;

/**
 * Active transaction status transitions.
 *
 * Every transaction starts as `AVAILABLE`.
 * - commit path:  `AVAILABLE -> ON_COMMIT`
 * - abort path:   `AVAILABLE -> ABORTED`
 *
 * `ON_COMMIT` is terminal for abort ownership: once commit owns the
 * transaction.
 */
pub struct ActiveState {
  tx_id: TxId,
  status: AtomicU8,
}
impl ActiveState {
  pub const fn new(tx_id: TxId) -> Self {
    Self {
      tx_id,
      status: AtomicU8::new(STATUS_AVAILABLE),
    }
  }
  pub fn is_available(&self) -> bool {
    self.status.load(Ordering::Relaxed) == STATUS_AVAILABLE
  }
  pub const fn get_id(&self) -> TxId {
    self.tx_id
  }

  pub fn try_abort(&self) -> bool {
    self
      .status
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_ABORTED,
        Ordering::Relaxed,
        Ordering::Relaxed,
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
        Ordering::Relaxed,
        Ordering::Relaxed,
      )
      .is_ok()
  }

  #[inline]
  pub fn make_available(&self) {
    self.status.store(STATUS_AVAILABLE, Ordering::Relaxed)
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
  inner: RwLock<BTreeMap<TxId, LazyLock<SBox<OnceParker>>>>,
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
    self.last_tx_id.load(Ordering::Relaxed)
  }

  /**
   * Allocate and publish a new active transaction under one write lock.
   *
   * A transaction id becomes externally observable only when its state is inserted
   * into the active map. Keeping id allocation and insertion in the same critical
   * section prevents a newly issued transaction from being missed by snapshots.
   */
  pub fn new_state(&self) -> ActiveState {
    let mut inner = self.inner.wl();

    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Relaxed);
    inner.insert(tx_id, LazyLock::new(Default::default));
    drop(inner);
    ActiveState::new(tx_id)
  }
  /**
   * Build a bitmap snapshot of active transaction ids below `max`.
   *
   * The snapshot is offset-based so visibility checks can test active ids without
   * storing every possible transaction id up to `max`.
   */
  pub fn snapshot_until(&self, max: TxId) -> OffsetBitmap {
    let inner = self.inner.rl();
    let Some((&offset, _)) = inner.first_key_value() else {
      return OffsetBitmap::new(0, 0);
    };
    let mut snapshot = OffsetBitmap::new(offset, max - offset + 1);
    for (id, _) in inner.range(..max) {
      snapshot.insert(*id);
    }
    snapshot
  }
  pub fn remove(&self, tx_id: &TxId) -> Option<SBox<OnceParker>> {
    let parker = self.inner.wl().remove(tx_id)?;
    LazyLock::get(&parker).cloned()
  }
  pub fn min_version(&self) -> Option<TxId> {
    self.inner.rl().first_key_value().map(|(k, _)| *k)
  }
  pub fn get(&self, tx_id: &TxId) -> Option<SBox<OnceParker>> {
    let inner = self.inner.rl();
    let parker = inner.get(tx_id)?;
    Some(SBox::clone(parker))
  }
}
