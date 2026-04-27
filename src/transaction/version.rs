use std::{
  fs,
  io::{IoSlice, Write},
  panic::RefUnwindSafe,
  path::{Path, PathBuf},
  sync::atomic::{AtomicU32, AtomicU8, Ordering},
};

use crossbeam_skiplist::{map::Entry, SkipMap, SkipSet};

use crate::{
  disk::{max_iov, Pread},
  utils::OffsetBitmap,
  wal::{AtomicTxId, TxId, TX_ID_BYTES},
  Error, Result,
};

const FILE_EXT: &str = "abt";

const STATUS_AVAILABLE: u8 = 0;
const STATUS_ON_COMMIT: u8 = 1; // Exclusive state during commit attempt — prevents timeout thread from aborting while WAL write is in progress
const STATUS_ABORTED: u8 = 2;
const STATUS_TIMEOUT: u8 = 3;

pub struct TxState<'a>(Entry<'a, TxId, AtomicU8>);
impl<'a> TxState<'a> {
  #[inline(always)]
  pub fn get_id(&self) -> TxId {
    *self.0.key()
  }
  #[inline]
  pub fn is_available(&self) -> bool {
    self.0.value().load(Ordering::Acquire) == STATUS_AVAILABLE
  }

  pub fn try_abort(&self) -> bool {
    let status = self.0.value();
    let current = status.load(Ordering::Acquire);
    if !matches!(current, STATUS_AVAILABLE | STATUS_TIMEOUT) {
      return false;
    }

    status
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
      .0
      .value()
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
      .0
      .value()
      .compare_exchange(
        STATUS_AVAILABLE,
        STATUS_ON_COMMIT,
        Ordering::Release,
        Ordering::Acquire,
      )
      .is_ok()
  }

  #[inline(always)]
  pub fn deactive(&self) {
    self.0.remove();
  }

  #[inline]
  pub fn make_available(&self) {
    self.0.value().store(STATUS_AVAILABLE, Ordering::Release)
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
  fn new(
    active: &SkipMap<TxId, AtomicU8>,
    aborted: &'a SkipSet<TxId>,
    max: TxId,
  ) -> Self {
    let front = match active.front() {
      Some(front) => front,
      _ => {
        return TxSnapshot {
          active: OffsetBitmap::new(0, 0),
          aborted,
        }
      }
    };

    let offset = *front.key();
    let mut snapshot = OffsetBitmap::new(offset, max - offset + 1);

    let mut entry = Some(front);
    while let Some(e) = entry.take_if(|e| *e.key() < max) {
      if !e.is_removed() {
        snapshot.insert(*e.key());
      }
      entry = e.next();
    }

    TxSnapshot {
      active: snapshot,
      aborted,
    }
  }

  #[inline]
  pub fn is_visible(&self, tx_id: &TxId) -> bool {
    !self.is_active(tx_id) && !self.aborted.contains(tx_id)
  }
  #[inline]
  pub fn is_active(&self, &tx_id: &TxId) -> bool {
    self.active.contains(tx_id)
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
  active: SkipMap<TxId, AtomicU8>,
  last_tx_id: AtomicTxId,
  base_path: PathBuf,
  file_id: AtomicU32,
}
impl VersionVisibility {
  pub fn new<T>(base_path: PathBuf, aborted: T, last_tx_id: TxId) -> Self
  where
    T: IntoIterator<Item = TxId>,
  {
    Self {
      base_path,
      active: Default::default(),
      aborted: SkipSet::from_iter(aborted),
      last_tx_id: AtomicTxId::new(last_tx_id),
      file_id: AtomicU32::new(0),
    }
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

  /**
   * Returns the oldest active tx_id, or the current version if no transaction is active.
   * Called before GC to determine the safe cleanup boundary — versions older than this
   * are not visible to any active reader and can be collected.
   */
  pub fn min_version(&self) -> TxId {
    self
      .active
      .front()
      .map(|v| *v.key())
      .unwrap_or_else(|| self.current_version())
  }
  #[inline]
  pub fn set_abort(&self, tx_id: TxId) {
    self.aborted.insert(tx_id);
  }
  pub fn new_transaction(&self) -> (TxState<'_>, TxSnapshot<'_>) {
    let tx_id = self.last_tx_id.fetch_add(1, Ordering::Release);
    (
      TxState(self.active.insert(tx_id, AtomicU8::new(STATUS_AVAILABLE))),
      TxSnapshot::new(&self.active, &self.aborted, tx_id),
    )
  }
  #[inline]
  pub fn current_version(&self) -> TxId {
    self.last_tx_id.load(Ordering::Acquire)
  }
  #[inline]
  pub fn get_active_state(&self, tx_id: TxId) -> Option<TxState<'_>> {
    self.active.get(&tx_id).map(TxState)
  }

  pub fn replay(&self, path: &Path) -> Result {
    let last = path
      .file_stem()
      .unwrap()
      .to_string_lossy()
      .parse::<u32>()
      .map_err(Error::unknown)?;
    self.file_id.store(last + 1, Ordering::Relaxed);

    let file = fs::OpenOptions::new()
      .read(true)
      .open(&path)
      .map_err(Error::IO)?;
    let mut offset = 0;
    let mut buf = vec![0; 4];
    file.pread(&mut buf, offset).map_err(Error::IO)?;
    let len = u32::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; 4]).read() });
    offset += 4;

    for _ in 0..len {
      let mut buf = vec![0; TX_ID_BYTES];
      file.pread(&mut buf, offset).map_err(Error::IO)?;
      offset += TX_ID_BYTES as u64;

      let id =
        TxId::from_le_bytes(unsafe { (buf.as_ptr() as *const [_; TX_ID_BYTES]).read() });
      self.aborted.insert(id);
    }

    self.clear(path)?;
    Ok(())
  }

  pub fn persist_aborted(&self, tx_id: TxId) -> Result<PathBuf> {
    let current = self
      .base_path
      .join(format!("{}", self.file_id.fetch_add(1, Ordering::Relaxed)))
      .with_extension(FILE_EXT);

    let mut file = fs::OpenOptions::new()
      .create(true)
      .write(true)
      .append(true)
      .open(&current)
      .map_err(Error::IO)?;
    let snapshot = self
      .aborted
      .range(..tx_id)
      .map(|v| v.value().to_le_bytes())
      .collect::<Vec<_>>();
    file
      .write(&(snapshot.len() as u32).to_le_bytes())
      .map_err(Error::IO)?;

    for chuck in snapshot.chunks(max_iov()) {
      let v = chuck
        .into_iter()
        .map(|v| IoSlice::new(v))
        .collect::<Vec<_>>();
      file.write_vectored(&v).map_err(Error::IO)?;
    }

    file.sync_data().map_err(Error::IO)?;

    Ok(current)
  }

  pub fn clear(&self, current: &Path) -> Result {
    for entry in fs::read_dir(&self.base_path).map_err(Error::IO)? {
      let path = entry.map_err(Error::IO)?.path();
      if path.extension().map_or(true, |ext| ext != FILE_EXT) {
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
