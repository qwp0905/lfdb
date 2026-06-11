use std::{
  collections::BTreeSet,
  ops::Deref,
  panic::RefUnwindSafe,
  path::{Path, PathBuf},
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
};

use crossbeam_skiplist::SkipSet;

use super::{ActiveSet, ActiveState};

use crate::{
  background::{EventBus, SharedSubscription},
  binding_events, debug,
  disk::IOPool,
  error, info,
  utils::{uuid_simple, OffsetBitmap, SBox},
  wal::{TxId, WALFailed, TX_ID_BYTES},
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
  pub fn current_version(&self) -> TxId {
    self.set.current_version()
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
  io_pool: Arc<IOPool>,
  closed: AtomicBool,
}
impl VersionVisibility {
  pub fn replay(
    io_pool: Arc<IOPool>,
    last_tx_id: TxId,
    started: BTreeSet<TxId>,
    closed: BTreeSet<TxId>,
    last_snapshot: Option<PathBuf>,
    event_bus: &EventBus,
  ) -> Result<Arc<Self>> {
    let (active_s, aborted_s) = match last_snapshot {
      Some(path) => Self::replay_snapshot(path, &io_pool)?,
      None => (BTreeSet::new(), BTreeSet::new()),
    };

    let this = Arc::new(Self {
      io_pool,
      aborted: active_s
        .into_iter()
        .chain(started)
        .chain(aborted_s)
        .filter(|c| !closed.contains(c))
        .collect(),
      active: ActiveSet::new(last_tx_id),
      closed: AtomicBool::new(false),
    });
    event_bus.register(&this);
    Ok(this)
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
      .unwrap_or_else(|| self.active.current_version())
  }
  #[inline]
  pub fn set_abort(&self, tx_id: TxId) {
    self.aborted.insert(tx_id);
  }
  pub fn new_transaction(&self) -> Option<(TxSnapshot<'_>, TxState<'_>)> {
    if self.closed.load(Ordering::Acquire) {
      return None;
    }
    let state = self.active.new_state();
    Some((
      TxSnapshot::new(self.active.snapshot_until(state.get_id()), &self.aborted),
      TxState::new(state, &self.active),
    ))
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
    let mut file = io_pool.open_scan_io(filename)?;
    debug!("snapshot opened.");

    let mut active = BTreeSet::new();
    let mut aborted = BTreeSet::new();

    let len = u32::from_le_bytes(file.read_to_vec(4)?.try_into().unwrap());
    for _ in 0..len {
      let id = TxId::from_le_bytes(file.read_to_vec(TX_ID_BYTES)?.try_into().unwrap());
      active.insert(id);
    }

    let len = u32::from_le_bytes(file.read_to_vec(4)?.try_into().unwrap());
    for _ in 0..len {
      let id = TxId::from_le_bytes(file.read_to_vec(TX_ID_BYTES)?.try_into().unwrap());
      aborted.insert(id);
    }
    debug!("snapshot replay completed.");
    Ok((active, aborted))
  }

  pub fn persist_snapshot(&self) -> Result<(TxId, PathBuf)> {
    let current = PathBuf::from(uuid_simple()).with_extension(FILE_EXT);
    let mut file = self.io_pool.open_append_io(current)?;
    let tx_id = self.active.current_version();

    let active = self
      .active
      .until(tx_id)
      .iter()
      .map(|v| v.to_le_bytes())
      .collect::<Vec<_>>();
    debug!("snapshot active ids {}", active.len());
    file.append(&(active.len() as u32).to_le_bytes())?;
    for bytes in active {
      file.append(&bytes)?;
    }

    let aborted = self
      .aborted
      .range(..tx_id)
      .map(|v| v.value().to_le_bytes())
      .collect::<Vec<_>>();
    debug!("snapshot aborted ids {}", aborted.len());
    file.append(&(aborted.len() as u32).to_le_bytes())?;
    for bytes in aborted {
      file.append(&bytes)?;
    }

    let path = file.flush()?;
    Ok((tx_id, path))
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
impl SharedSubscription<WALFailed> for VersionVisibility {
  fn handle(&self, _: Arc<WALFailed>) {
    if self.closed.fetch_or(true, Ordering::Release) {
      return;
    }
    for state in self.active.get_all().into_iter().filter(|v| v.try_abort()) {
      self.aborted.insert(state.get_id());
      self.active.remove(&state.get_id());
    }
    error!("all versions transit to abort since wal failure detected.");
  }
}
binding_events!(VersionVisibility {
  shared: [WALFailed]
});

impl RefUnwindSafe for VersionVisibility {}
