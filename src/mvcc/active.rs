use std::{
  collections::{btree_map::Entry, BTreeMap},
  sync::{atomic::Ordering, RwLock},
};

use crate::{
  background::OnceParker,
  utils::{OffsetBitmap, SBox, ShortenedRwLock},
  wal::{AtomicTxId, TxId},
};

pub struct WritableState {
  id: TxId,
  parker: OnceParker,
}
impl WritableState {
  const fn new(id: TxId) -> Self {
    Self {
      id,
      parker: OnceParker::new(),
    }
  }
  pub const fn get_id(&self) -> TxId {
    self.id
  }
  pub fn park(&self) {
    self.parker.park();
  }
  pub fn wake_all(&self) {
    self.parker.wake_all();
  }
}
pub struct ReadonlyState {
  upper_bound: TxId,
  snapshot: OffsetBitmap,
}
impl ReadonlyState {
  const fn new(upper_bound: TxId, snapshot: OffsetBitmap) -> Self {
    Self {
      upper_bound,
      snapshot,
    }
  }
  pub const fn get_upper_bound(&self) -> TxId {
    self.upper_bound
  }

  pub fn contains_in_snapshot(&self, tx_id: TxId) -> bool {
    self.snapshot.contains(tx_id)
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
  readonly: RwLock<BTreeMap<TxId, usize>>,
  writable: RwLock<BTreeMap<TxId, SBox<WritableState>>>,
  last_tx_id: AtomicTxId,
}
impl ActiveSet {
  pub const fn new(last_tx_id: TxId) -> Self {
    Self {
      readonly: RwLock::new(BTreeMap::new()),
      writable: RwLock::new(BTreeMap::new()),
      last_tx_id: AtomicTxId::new(last_tx_id),
    }
  }
  pub fn current_version(&self) -> TxId {
    self.last_tx_id.load(Ordering::Relaxed)
  }

  pub fn new_writable(&self) -> SBox<WritableState> {
    let mut uninit = SBox::new_uninit();
    let mut writable = self.writable.wl();

    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Relaxed);
    SBox::get_mut(&mut uninit)
      .unwrap_or_else(|| unreachable!())
      .write(WritableState::new(tx_id));

    writable
      .entry(tx_id)
      .and_modify(|_| unreachable!())
      .or_insert(unsafe { uninit.assume_init() })
      .clone()
  }
  pub fn new_readonly(&self) -> ReadonlyState {
    let mut readonly = self.readonly.wl();
    let upper_bound = self.last_tx_id.load(Ordering::Relaxed);
    *readonly.entry(upper_bound).or_insert(0) += 1;
    drop(readonly);

    let writable = self.writable.rl();
    let Some((&offset, _)) = writable.first_key_value() else {
      return ReadonlyState::new(upper_bound, OffsetBitmap::new(0, 0));
    };
    let mut snapshot = OffsetBitmap::new(offset, upper_bound - offset + 1);
    for (id, _) in writable.range(..upper_bound) {
      snapshot.insert(*id);
    }
    ReadonlyState::new(upper_bound, snapshot)
  }

  pub fn snapshot_until(&self) -> (TxId, Vec<TxId>) {
    let writable = self.writable.rl();
    let max = self.current_version();
    let mut snapshot = Vec::new();
    for (id, _) in writable.range(..max) {
      snapshot.push(*id);
    }
    (max, snapshot)
  }

  pub fn remove_readonly(&self, tx_id: TxId) {
    let mut readonly = self.readonly.wl();
    let Entry::Occupied(mut entry) = readonly.entry(tx_id) else {
      return;
    };
    let count = *entry.get();
    if count > 1 {
      entry.insert(count - 1);
      return;
    }
    entry.remove();
  }
  pub fn remove_writable(&self, tx_id: TxId) -> Option<SBox<WritableState>> {
    self.writable.wl().remove(&tx_id)
  }
  pub fn min_version(&self) -> TxId {
    let readonly = self.readonly.rl();
    if let Some((id, _)) = readonly.first_key_value() {
      return *id;
    }
    self.current_version()
  }
  pub fn get_writable(&self, tx_id: &TxId) -> Option<SBox<WritableState>> {
    self.writable.rl().get(tx_id).cloned()
  }
}
