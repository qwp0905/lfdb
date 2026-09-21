use std::{collections::BTreeSet, sync::RwLock};

use crate::{
  utils::{OffsetBitmap, ShortenedRwLock},
  wal::TxId,
};

pub struct AbortedSet(RwLock<OffsetBitmap>);
impl AbortedSet {
  pub fn empty() -> Self {
    Self(RwLock::new(OffsetBitmap::new(0, 0)))
  }

  pub fn from_exists(exists: BTreeSet<TxId>) -> Self {
    let Some(offset) = exists.first().copied() else {
      return Self::empty();
    };
    let max = exists.last().copied().unwrap_or_else(|| unreachable!());
    let mut bitmap = OffsetBitmap::new(offset, max - offset + 1);
    for i in exists {
      bitmap.insert(i);
    }
    Self(RwLock::new(bitmap))
  }

  pub fn contains(&self, tx_id: TxId) -> bool {
    self.0.rl().contains(tx_id)
  }

  pub fn insert(&self, tx_id: TxId) {
    let mut bitmap = self.0.wl();
    bitmap.ensure_capacity(tx_id);
    bitmap.insert(tx_id);
  }

  pub fn remove_until(&self, tx_id: TxId) {
    let mut bitmap = self.0.wl();
    bitmap.advance_offset(tx_id);
  }

  pub fn snapshot_until(&self, max: TxId) -> Vec<TxId> {
    self.0.rl().iter().take_while(|&i| i < max).collect()
  }
}
