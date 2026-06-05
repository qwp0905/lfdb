use std::{
  collections::BTreeSet,
  mem::transmute,
  ops::Deref,
  panic::RefUnwindSafe,
  path::{Path, PathBuf},
  sync::{atomic::Ordering, Arc},
};

use crossbeam_skiplist::SkipSet;

use super::{ActiveSet, ActiveState};

use crate::{
  debug,
  disk::{max_iov, IOPool},
  info,
  utils::{uuid_simple, OffsetBitmap, SBox},
  wal::{AtomicTxId, TxId, TX_ID_BYTES},
  Result,
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
  io_pool: Arc<IOPool>,
}
impl VersionVisibility {
  pub fn replay(
    io_pool: Arc<IOPool>,
    last_tx_id: TxId,
    aborted: BTreeSet<TxId>,
    started: BTreeSet<TxId>,
    closed: BTreeSet<TxId>,
    last_snapshot: Option<PathBuf>,
  ) -> Result<Self> {
    let (active_s, aborted_s) = match last_snapshot {
      Some(path) => Self::replay_snapshot(path, &io_pool)?,
      None => (BTreeSet::new(), BTreeSet::new()),
    };
    Ok(Self {
      io_pool,
      aborted: active_s
        .into_iter()
        .chain(started)
        .chain(aborted_s)
        .chain(aborted)
        .filter(|c| !closed.contains(c))
        .collect(),
      active: ActiveSet::new(),
      last_tx_id: AtomicTxId::new(last_tx_id),
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

  fn replay_snapshot(
    filename: PathBuf,
    io_pool: &IOPool,
  ) -> Result<(BTreeSet<TxId>, BTreeSet<TxId>)> {
    info!("trying to open snapshot {:?}", filename);
    let file = io_pool.open_buffered_io(filename)?;
    debug!("snapshot opened.");

    let mut active = BTreeSet::new();
    let mut aborted = BTreeSet::new();

    let mut offset = 0;
    let mut buf = vec![0; 4];
    file.read(&mut buf, offset)?;
    let len = u32::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; 4]).read() });
    offset += 4;

    for _ in 0..len {
      let mut buf = vec![0; TX_ID_BYTES];
      file.read(&mut buf, offset)?;
      offset += TX_ID_BYTES as u64;

      let id =
        TxId::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; TX_ID_BYTES]).read() });
      active.insert(id);
    }

    file.read(&mut buf, offset)?;
    let len = u32::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; 4]).read() });
    offset += 4;

    for _ in 0..len {
      let mut buf = vec![0; TX_ID_BYTES];
      file.read(&mut buf, offset)?;
      offset += TX_ID_BYTES as u64;

      let id =
        TxId::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; TX_ID_BYTES]).read() });
      aborted.insert(id);
    }
    debug!("snapshot replay completed.");
    Ok((active, aborted))
  }

  pub fn persist_snapshot(&self, tx_id: TxId) -> Result<PathBuf> {
    let current = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);
    let file = self.io_pool.open_buffered_io(current)?;

    let mut offset = 0;
    let active = self
      .active
      .until(tx_id)
      .iter()
      .map(|v| v.to_le_bytes())
      .collect::<Vec<_>>();
    let len = (active.len() as u32).to_le_bytes();
    file
      .write_async(unsafe { transmute(len.as_slice()) }, offset)
      .wait()?;
    offset += 4;
    for chunk in active.chunks(max_iov()) {
      let mut waiting = Vec::with_capacity(chunk.len());
      for v in chunk {
        waiting.push(file.write_async(unsafe { transmute(v.as_slice()) }, offset));
        offset += (TX_ID_BYTES * v.len()) as u64;
      }
      waiting.into_iter().map(|d| d.wait()).collect::<Result>()?;
    }

    let aborted = self
      .aborted
      .range(..tx_id)
      .map(|v| v.value().to_le_bytes())
      .collect::<Vec<_>>();
    let len = (aborted.len() as u32).to_le_bytes();
    file
      .write_async(unsafe { transmute(len.as_slice()) }, offset)
      .wait()?;
    offset += 4;

    for chunk in aborted.chunks(max_iov()) {
      let mut waiting = Vec::with_capacity(chunk.len());
      for v in chunk {
        waiting.push(file.write_async(unsafe { transmute(v.as_slice()) }, offset));
        offset += (TX_ID_BYTES * v.len()) as u64;
      }
      waiting.into_iter().map(|d| d.wait()).collect::<Result>()?;
    }

    file.fsync()?;
    Ok(file.filename())
  }

  pub fn clear(&self, current: &Path) -> Result {
    for entry in self.io_pool.read_dir()? {
      let path = PathBuf::from(entry.file_name());
      if path.extension().is_none_or(|ext| ext != FILE_EXT) {
        continue;
      };
      if path == current {
        continue;
      }
      self.io_pool.remove(&path)?;
    }
    Ok(())
  }
}
impl RefUnwindSafe for VersionVisibility {}
