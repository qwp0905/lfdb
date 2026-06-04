use std::{
  collections::BTreeSet,
  fs,
  io::IoSlice,
  ops::Deref,
  panic::RefUnwindSafe,
  path::{Path, PathBuf},
  sync::atomic::Ordering,
};

use crossbeam_skiplist::SkipSet;
use uuid::Uuid;

use super::{ActiveSet, ActiveState};

use crate::{
  disk::{max_iov, Pread, Pwrite, Pwritev},
  utils::{OffsetBitmap, SBox},
  wal::{AtomicTxId, TxId, TX_ID_BYTES},
  Error, Result,
};

const FILE_EXT: &str = "snap";

pub struct TxState<'a> {
  state: SBox<ActiveState>,
  set: &'a ActiveSet,
}
impl<'a> TxState<'a> {
  const fn new(state: SBox<ActiveState>, set: &'a ActiveSet) -> Self {
    Self { state, set }
  }
  pub fn deactive(&self) {
    self.set.remove(&self.state.get_id());
  }
}
impl<'a> Deref for TxState<'a> {
  type Target = ActiveState;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

/**
 * Snapshot of the version visibility active set to achieve snapshot isolation.
 */
pub struct TxSnapshot<'a> {
  active: OffsetBitmap,
  aborted: &'a SkipSet<TxId>,
}
impl<'a> TxSnapshot<'a> {
  fn new(active: OffsetBitmap, aborted: &'a SkipSet<TxId>) -> Self {
    Self { active, aborted }
  }

  #[inline]
  pub fn is_active(&self, &tx_id: &TxId) -> bool {
    self.active.contains(tx_id)
  }
  pub fn is_aborted(&self, tx_id: &TxId) -> bool {
    self.aborted.contains(tx_id)
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
  aborted: SkipSet<TxId>,
  active: ActiveSet,
  last_tx_id: AtomicTxId,
  base_path: PathBuf,
}
impl VersionVisibility {
  pub fn replay(
    base_path: PathBuf,
    last_tx_id: TxId,
    aborted: BTreeSet<TxId>,
    started: BTreeSet<TxId>,
    closed: BTreeSet<TxId>,
    last_snapshot: Option<PathBuf>,
  ) -> Result<Self> {
    let (active_s, aborted_s) = match last_snapshot {
      Some(path) => Self::replay_snapshot(&path)?,
      None => (BTreeSet::new(), BTreeSet::new()),
    };
    Ok(Self {
      aborted: active_s
        .into_iter()
        .chain(started)
        .chain(aborted_s)
        .chain(aborted)
        .filter(|c| !closed.contains(c))
        .collect(),
      active: ActiveSet::new(),
      last_tx_id: AtomicTxId::new(last_tx_id),
      base_path,
    })
  }

  /**
   * Trims aborted tx_ids that are older than version. Called after GC completes —
   * version is the oldest tx_id that GC has fully cleaned up, so no active reader
   * can reference those versions anymore and their abort status no longer needs tracking.
   */
  pub fn remove_aborted(&self, version: &TxId) {
    while let Some(v) = self.aborted.front() {
      if v.value() >= version {
        return;
      }
      v.remove();
    }
  }

  #[inline]
  pub fn is_aborted(&self, tx_id: &TxId) -> bool {
    self.aborted.contains(tx_id)
  }

  pub fn wait_commit(&self, owner: TxId) {
    self.active.wait(&owner)
  }

  /**
   * Returns the oldest active tx_id, or the current version if no transaction is active.
   * Called before GC to determine the safe cleanup boundary — versions older than this
   * are not visible to any active reader and can be collected.
   */
  pub fn min_version(&self) -> TxId {
    self
      .active
      .min_version()
      .unwrap_or_else(|| self.current_version())
  }
  #[inline]
  pub fn set_abort(&self, tx_id: TxId) {
    self.aborted.insert(tx_id);
  }
  pub fn new_transaction(&self) -> (TxState<'_>, TxSnapshot<'_>) {
    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    let state = SBox::new(ActiveState::new(tx_id));
    self.active.insert(state.clone());
    (
      TxState::new(state, &self.active),
      TxSnapshot::new(self.active.snapshot_until(tx_id), &self.aborted),
    )
  }
  #[inline]
  pub fn current_version(&self) -> TxId {
    self.last_tx_id.load(Ordering::Acquire)
  }
  #[inline]
  pub fn get_active_state(&self, tx_id: TxId) -> Option<TxState<'_>> {
    self
      .active
      .get(&tx_id)
      .map(|state| TxState::new(state, &self.active))
  }

  fn replay_snapshot(path: &Path) -> Result<(BTreeSet<TxId>, BTreeSet<TxId>)> {
    let mut active = BTreeSet::new();
    let mut aborted = BTreeSet::new();

    let file = fs::OpenOptions::new()
      .read(true)
      .open(&path)
      .map_err(Error::IO)?;
    let mut offset = 0;
    let mut buf = vec![0; 4];
    file.pread_exact(&mut buf, offset).map_err(Error::IO)?;
    let len = u32::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; 4]).read() });
    offset += 4;

    for _ in 0..len {
      let mut buf = vec![0; TX_ID_BYTES];
      file.pread_exact(&mut buf, offset).map_err(Error::IO)?;
      offset += TX_ID_BYTES as u64;

      let id =
        TxId::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; TX_ID_BYTES]).read() });
      active.insert(id);
    }

    file.pread_exact(&mut buf, offset).map_err(Error::IO)?;
    let len = u32::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; 4]).read() });
    offset += 4;

    for _ in 0..len {
      let mut buf = vec![0; TX_ID_BYTES];
      file.pread_exact(&mut buf, offset).map_err(Error::IO)?;
      offset += TX_ID_BYTES as u64;

      let id =
        TxId::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; TX_ID_BYTES]).read() });
      aborted.insert(id);
    }

    Ok((active, aborted))
  }

  pub fn persist_snapshot(&self, tx_id: TxId) -> Result<PathBuf> {
    let current = self
      .base_path
      .join(Uuid::new_v4().to_string())
      .with_extension(FILE_EXT);

    let file = fs::OpenOptions::new()
      .create(true)
      .write(true)
      .open(&current)
      .map_err(Error::IO)?;

    let mut offset = 0;

    let active = self
      .active
      .until(tx_id)
      .iter()
      .map(|v| v.to_le_bytes())
      .collect::<Vec<_>>();
    file
      .pwrite_all(&(active.len() as u32).to_le_bytes(), offset)
      .map_err(Error::IO)?;
    offset += 4;
    for chuck in active.chunks(max_iov()) {
      let mut v = chuck
        .into_iter()
        .map(|v| IoSlice::new(v))
        .collect::<Vec<_>>();
      file.pwritev_all(&mut v, offset).map_err(Error::IO)?;
      offset += (TX_ID_BYTES * v.len()) as u64;
    }

    let aborted = self
      .aborted
      .range(..tx_id)
      .map(|v| v.value().to_le_bytes())
      .collect::<Vec<_>>();
    file
      .pwrite_all(&(aborted.len() as u32).to_le_bytes(), offset)
      .map_err(Error::IO)?;
    offset += 4;

    for chuck in aborted.chunks(max_iov()) {
      let mut v = chuck
        .into_iter()
        .map(|v| IoSlice::new(v))
        .collect::<Vec<_>>();
      file.pwritev_all(&mut v, offset).map_err(Error::IO)?;
      offset += (TX_ID_BYTES * v.len()) as u64;
    }

    file.sync_data().map_err(Error::IO)?;

    Ok(current)
  }

  pub fn clear(&self, current: &Path) -> Result {
    for entry in fs::read_dir(&self.base_path).map_err(Error::IO)? {
      let path = entry.map_err(Error::IO)?.path();
      if path.extension().is_none_or(|ext| ext != FILE_EXT) {
        continue;
      };
      if path == current {
        continue;
      }
      fs::remove_file(path).map_err(Error::IO)?;
    }
    Ok(())
  }
}
impl RefUnwindSafe for VersionVisibility {}
