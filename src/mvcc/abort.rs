use std::{collections::BTreeSet, sync::RwLock};

use crate::{
  utils::{OffsetBitmap, ShortenedRwLock},
  wal::TxId,
};

pub struct AbortedSet(RwLock<OffsetBitmap>);
impl AbortedSet {
  pub fn empty() -> Self {
    Self(RwLock::new(OffsetBitmap::new(0)))
  }

  pub fn from_exists(exists: BTreeSet<TxId>) -> Self {
    let Some(offset) = exists.first().copied() else {
      return Self::empty();
    };
    let mut bitmap = OffsetBitmap::new(offset);
    for i in exists {
      bitmap.insert(i);
    }
    Self(RwLock::new(bitmap))
  }

  pub fn contains(&self, tx_id: TxId) -> bool {
    self.0.rl().contains(tx_id)
  }

  pub fn insert(&self, tx_id: TxId) {
    self.0.wl().insert(tx_id);
  }

  pub fn remove_until(&self, tx_id: TxId) {
    self.0.wl().remove_until(tx_id);
  }

  pub fn snapshot_until(&self, max: TxId) -> Vec<TxId> {
    self.0.rl().iter().take_while(|&i| i < max).collect()
  }
}
