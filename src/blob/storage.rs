use crate::{
  cache::ShrinkMap,
  debug,
  disk::{AlignedBuf, IOPool},
  utils::{uuid_simple, SBox, Semaphore, ShortenedRwLock},
  wal::WriteAheadLog,
  Result,
};

use super::{BlobHandle, BlobId, BlobLen, BlobMetadata, BlobOffset, BlobReserved};
use std::{
  collections::{HashMap, HashSet},
  path::PathBuf,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
  },
};

const FILE_EXT: &str = "blob";
fn filename() -> PathBuf {
  PathBuf::from(uuid_simple()).with_extension(FILE_EXT)
}

const MAX_APPEND: u32 = 5;

/**
 * Blob segment registry.
 *
 * Blob storage manages fixed-size blob segments in two sets: writable segments
 * that can still accept reservations, and readonly segments that are sealed and
 * can be considered by GC. Appending writes and syncs the blob bytes before
 * returning the blob reference.
 */
pub struct BlobStorage {
  readonly: RwLock<ShrinkMap<BlobId, SBox<BlobHandle>>>,
  writable: RwLock<ShrinkMap<BlobId, SBox<BlobHandle>>>,
  last_id: AtomicU64,
  io_pool: Arc<IOPool>,
  wal: Arc<WriteAheadLog>,
  append_gate: Semaphore,
}
impl BlobStorage {
  pub fn replay(
    handles: Vec<BlobMetadata>,
    io_pool: Arc<IOPool>,
    wal: Arc<WriteAheadLog>,
  ) -> Result<Self> {
    // Writable state is runtime-only. After recovery, existing blob files are sealed
    // as readonly because the storage layer does not try to reconstruct the exact
    // append frontier inside each blob segment. Any unused tail space is accepted as
    // fragmentation.

    let mut opened = HashSet::new();
    let mut readonly = ShrinkMap::new();
    let mut last_id = 0;
    for metadata in handles {
      if !opened.insert(metadata.get_filename().clone()) {
        continue;
      };

      let id = metadata.get_id();
      last_id = last_id.max(id + 1);

      let handle = BlobHandle::new(
        io_pool.open_direct_io(metadata.get_filename().clone())?,
        metadata,
      );
      readonly.insert(id, SBox::new(handle));
    }

    for entry in io_pool.read_dir()? {
      let filename = PathBuf::from(entry.file_name());
      if filename.extension().is_none_or(|ext| ext != FILE_EXT) {
        continue;
      }
      if opened.contains(&filename) {
        continue;
      }
      io_pool.truncate(&filename)?;
    }

    Ok(Self {
      readonly: RwLock::new(readonly),
      writable: RwLock::new(ShrinkMap::new()),
      last_id: AtomicU64::new(last_id),
      io_pool,
      wal,
      append_gate: Semaphore::new(MAX_APPEND),
    })
  }

  pub fn readonly_handle_ids(&self) -> Vec<BlobId> {
    self
      .readonly
      .rl()
      .values()
      .map(|h| h.metadata().get_id())
      .collect()
  }
  fn writable_handles(&self) -> Vec<SBox<BlobHandle>> {
    self.writable.rl().values().cloned().collect()
  }

  pub fn truncate(&self, blob_id: BlobId) -> Result {
    debug!("blob {blob_id} unreachable.");
    let Some(handle) = self.readonly.wl().remove(&blob_id) else {
      return Ok(());
    };
    handle.truncate()
  }

  pub fn get(&self, blob_id: BlobId) -> Option<SBox<BlobHandle>> {
    self
      .readonly
      .rl()
      .get(&blob_id)
      .cloned()
      .or_else(|| self.writable.rl().get(&blob_id).cloned())
  }

  pub fn append(&self, buf: Vec<u8>) -> Result<BlobAppendGuard<'_>> {
    let buf = AlignedBuf::from_vec(buf);
    let len = buf.len();
    let size = buf.size() as BlobOffset;

    loop {
      let permit = self.append_gate.acquire();

      for handle in self.writable_handles() {
        match handle.reserve(size) {
          BlobReserved::Ok(offset) => {
            drop(permit);
            handle.write(&buf, offset)?;
            // Blob payloads are outside the WAL durability boundary. Sync the blob bytes
            // before returning a reference that may be persisted into the tree.
            handle.sync().wait()?;
            return Ok(BlobAppendGuard::new(
              handle.metadata().get_id(),
              offset,
              len as BlobLen,
              None,
            ));
          }
          BlobReserved::Last(offset) => {
            drop(permit);
            handle.write(&buf, offset)?;
            handle.sync().wait()?;
            return Ok(BlobAppendGuard::new(
              handle.metadata().get_id(),
              offset,
              len as BlobLen,
              Some((handle, self)),
            ));
          }
          BlobReserved::Eof => continue,
        }
      }

      let new = self.open_new_handle()?;

      // The newly created blob file must be visible after a crash so replay/GC can
      // discover and account for it. Blob contents and directory namespace durability
      // are handled as separate boundaries.
      self.io_pool.sync_dir()?;
      self.wal.append_blob_created(new.metadata().clone())?;
      self
        .writable
        .wl()
        .insert(new.metadata().get_id(), SBox::new(new));
    }
  }

  fn open_new_handle(&self) -> Result<BlobHandle> {
    let id = self.last_id.fetch_add(1, Ordering::Relaxed);
    let metadata = BlobMetadata::new(id, filename());

    let io = self
      .io_pool
      .open_direct_io(metadata.get_filename().clone())?;
    Ok(BlobHandle::new(io, metadata))
  }

  pub fn metadata_snapshot(&self) -> Vec<BlobMetadata> {
    let mut handles = HashMap::new();
    for handle in self.writable.rl().values() {
      handles.insert(handle.metadata().get_id(), handle.metadata().clone());
    }
    for handle in self.readonly.rl().values() {
      handles
        .entry(handle.metadata().get_id())
        .or_insert_with(|| handle.metadata().clone());
    }

    handles.into_values().collect()
  }
}

/**
 * Guard returned after appending a blob value.
 *
 * When an append fills a segment past the writable threshold, the segment must
 * not become readonly until the caller has had a chance to persist the returned
 * blob reference into the tree. Otherwise GC could observe the sealed segment
 * before any reachable record points at the new blob. Dropping the guard seals
 * the segment by moving it from writable to readonly.
 */
pub struct BlobAppendGuard<'a> {
  id: BlobId,
  offset: BlobOffset,
  len: BlobLen,
  handle: Option<(SBox<BlobHandle>, &'a BlobStorage)>,
}
impl<'a> BlobAppendGuard<'a> {
  const fn new(
    id: BlobId,
    offset: BlobOffset,
    len: BlobLen,
    handle: Option<(SBox<BlobHandle>, &'a BlobStorage)>,
  ) -> Self {
    Self {
      id,
      offset,
      len,
      handle,
    }
  }
  pub const fn get_id(&self) -> BlobId {
    self.id
  }
  pub const fn get_offset(&self) -> BlobOffset {
    self.offset
  }
  pub const fn get_len(&self) -> BlobLen {
    self.len
  }
}
impl<'a> Drop for BlobAppendGuard<'a> {
  fn drop(&mut self) {
    let Some((handle, storage)) = self.handle.take() else {
      return;
    };
    let id = handle.metadata().get_id();
    storage.readonly.wl().insert(id, handle.clone());
    storage.writable.wl().remove(&id);
  }
}
